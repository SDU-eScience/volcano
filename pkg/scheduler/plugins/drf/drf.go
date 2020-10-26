/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package drf

import (
	"math"

	"k8s.io/klog"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/helpers"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// PluginName indicates name of volcano scheduler plugin.
const PluginName = "drf"

var shareDelta = 0.000001

type drfAttr struct {
	share            float64
	dominantResource string
	allocated        *api.Resource
}

type drfPlugin struct {
	totalResource *api.Resource

	// Key is Job ID
	jobAttrs map[api.JobID]*drfAttr

	// map[namespaceName]->attr
	namespaceOpts map[string]*drfAttr

	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

// New return drf plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &drfPlugin{
		totalResource:   api.EmptyResource(),
		jobAttrs:        map[api.JobID]*drfAttr{},
		namespaceOpts:   map[string]*drfAttr{},
		pluginArguments: arguments,
	}
}

func (drf *drfPlugin) Name() string {
	return PluginName
}

// NamespaceOrderEnabled returns the NamespaceOrder for this plugin is enabled in this session or not
func (drf *drfPlugin) NamespaceOrderEnabled(ssn *framework.Session) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if plugin.Name != PluginName {
				continue
			}
			return plugin.EnabledNamespaceOrder != nil && *plugin.EnabledNamespaceOrder
		}
	}
	return false
}

func (drf *drfPlugin) OnSessionOpen(ssn *framework.Session) {
	// Prepare scheduling data for this session.
	for _, n := range ssn.Nodes {
		drf.totalResource.Add(n.Allocatable)
	}

	namespaceOrderEnabled := drf.NamespaceOrderEnabled(ssn)

	for _, job := range ssn.Jobs {
		attr := &drfAttr{
			allocated: api.EmptyResource(),
		}

		for status, tasks := range job.TaskStatusIndex {
			if api.AllocatedStatus(status) {
				for _, t := range tasks {
					attr.allocated.Add(t.Resreq)
				}
			}
		}

		// Calculate the init share of Job
		drf.updateJobShare(job.GetUser(), job.Name, attr)

		drf.jobAttrs[job.UID] = attr

		if namespaceOrderEnabled {
			nsOpts, found := drf.namespaceOpts[job.GetUser()]
			if !found {
				nsOpts = &drfAttr{
					allocated: api.EmptyResource(),
				}
				drf.namespaceOpts[job.GetUser()] = nsOpts
			}
			// all task in job should have the same namespace with job
			nsOpts.allocated.Add(attr.allocated)
			drf.updateNamespaceShare(job.GetUser(), nsOpts)
		}
	}

	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) []*api.TaskInfo {
		var victims []*api.TaskInfo

		addVictim := func(candidate *api.TaskInfo) {
			victims = append(victims, candidate)
		}

		if namespaceOrderEnabled {
			// apply the namespace share policy on preemptee firstly

			lWeight := ssn.NamespaceInfo[api.NamespaceName(GetUser(preemptor, ssn))].GetWeight()
			lNsAtt := drf.namespaceOpts[GetUser(preemptor, ssn)]
			lNsAlloc := lNsAtt.allocated.Clone().Add(preemptor.Resreq)
			_, lNsShare := drf.calculateShare(lNsAlloc, drf.totalResource)
			lNsShareWeighted := lNsShare / float64(lWeight)

			namespaceAllocation := map[string]*api.Resource{}

			// undecidedPreemptees means this policy could not judge preemptee is preemptable or not
			// and left it to next policy
			undecidedPreemptees := []*api.TaskInfo{}

			for _, preemptee := range preemptees {
				if GetUser(preemptor, ssn) == GetUser(preemptee, ssn) {
					// policy is disabled when they are in the same namespace
					undecidedPreemptees = append(undecidedPreemptees, preemptee)
					continue
				}

				// compute the preemptee namespace weighted share after preemption
				nsAllocation, found := namespaceAllocation[GetUser(preemptee, ssn)]
				if !found {
					rNsAtt := drf.namespaceOpts[GetUser(preemptee, ssn)]
					nsAllocation = rNsAtt.allocated.Clone()
					namespaceAllocation[GetUser(preemptee, ssn)] = nsAllocation
				}
				rWeight := ssn.NamespaceInfo[api.NamespaceName(GetUser(preemptee, ssn))].GetWeight()
				rNsAlloc := nsAllocation.Sub(preemptee.Resreq)
				_, rNsShare := drf.calculateShare(rNsAlloc, drf.totalResource)
				rNsShareWeighted := rNsShare / float64(rWeight)

				// to avoid ping pong actions, the preemptee namespace should
				// have the higher weighted share after preemption.
				if lNsShareWeighted < rNsShareWeighted {
					addVictim(preemptee)
					continue
				}
				if lNsShareWeighted-rNsShareWeighted > shareDelta {
					continue
				}

				// equal namespace order leads to judgement of jobOrder
				undecidedPreemptees = append(undecidedPreemptees, preemptee)
			}

			preemptees = undecidedPreemptees
		}

		latt := drf.jobAttrs[preemptor.Job]
		lalloc := latt.allocated.Clone().Add(preemptor.Resreq)
		_, ls := drf.calculateShare(lalloc, drf.totalResource)

		allocations := map[api.JobID]*api.Resource{}

		for _, preemptee := range preemptees {
			if _, found := allocations[preemptee.Job]; !found {
				ratt := drf.jobAttrs[preemptee.Job]
				allocations[preemptee.Job] = ratt.allocated.Clone()
			}
			ralloc := allocations[preemptee.Job].Sub(preemptee.Resreq)
			_, rs := drf.calculateShare(ralloc, drf.totalResource)

			if ls < rs || math.Abs(ls-rs) <= shareDelta {
				addVictim(preemptee)
			}
		}

		klog.V(4).Infof("Victims from DRF plugins are %+v", victims)

		return victims
	}

	ssn.AddPreemptableFn(drf.Name(), preemptableFn)

	namespaceOrderFnBase := func(lUser string, rUser string) int {
		lOpt := drf.namespaceOpts[lUser]
		rOpt := drf.namespaceOpts[rUser]

		lWeight := ssn.NamespaceInfo[api.NamespaceName(lUser)].GetWeight()
		rWeight := ssn.NamespaceInfo[api.NamespaceName(rUser)].GetWeight()

		klog.V(3).Infof("DRF NamespaceOrderFn: <%v> share state: %f, weight %v, <%v> share state: %f, weight %v",
			lUser, lOpt.share, lWeight, rUser, rOpt.share, rWeight)

		lWeightedShare := lOpt.share / float64(lWeight)
		rWeightedShare := rOpt.share / float64(rWeight)

		metrics.UpdateNamespaceWeight(lUser, lWeight)
		metrics.UpdateNamespaceWeight(rUser, rWeight)
		metrics.UpdateNamespaceWeightedShare(lUser, lWeightedShare)
		metrics.UpdateNamespaceWeightedShare(rUser, rWeightedShare)

		if lWeightedShare > rWeightedShare {
			return 1
		}
		if lWeightedShare < rWeightedShare {
			return -1
		}

		return 0
	}

	jobOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		klog.V(4).Infof("DRF JobOrderFn: <%v/%v> share state: %v",
			lv.Namespace, lv.Name, drf.jobAttrs[lv.UID].share)

		klog.V(4).Infof("DRF JobOrderFn: <%v/%v> share state: %v",
			rv.Namespace, rv.Name, drf.jobAttrs[rv.UID].share)

		nsv := namespaceOrderFnBase(lv.GetUser(), rv.GetUser())
		if nsv != 0 {
			return nsv
		}

		if drf.jobAttrs[lv.UID].share == drf.jobAttrs[rv.UID].share {
			return 0
		}

		if drf.jobAttrs[lv.UID].share < drf.jobAttrs[rv.UID].share {
			return -1
		}

		return 1
	}

	ssn.AddJobOrderFn(drf.Name(), jobOrderFn)

	namespaceOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(api.NamespaceName)
		rv := r.(api.NamespaceName)

		return namespaceOrderFnBase(string(lv), string(rv))
	}

	if namespaceOrderEnabled {
		ssn.AddNamespaceOrderFn(drf.Name(), namespaceOrderFn)
	}

	// Register event handlers.
	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: func(event *framework.Event) {
			attr := drf.jobAttrs[event.Task.Job]
			attr.allocated.Add(event.Task.Resreq)

			job := ssn.Jobs[event.Task.Job]
			drf.updateJobShare(job.GetUser(), job.Name, attr)

			nsShare := -1.0
			if namespaceOrderEnabled {
				nsOpt := drf.namespaceOpts[GetUser(event.Task, ssn)]
				nsOpt.allocated.Add(event.Task.Resreq)

				drf.updateNamespaceShare(GetUser(event.Task, ssn), nsOpt)
				nsShare = nsOpt.share
			}

			klog.V(3).Infof("DRF AllocateFunc: task <%v/%v>, resreq <%v>,  share <%v>, namespace share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share, nsShare)
		},
		DeallocateFunc: func(event *framework.Event) {
			attr := drf.jobAttrs[event.Task.Job]
			attr.allocated.Sub(event.Task.Resreq)

			job := ssn.Jobs[event.Task.Job]
			drf.updateJobShare(job.GetUser(), job.Name, attr)

			nsShare := -1.0
			if namespaceOrderEnabled {
				nsOpt := drf.namespaceOpts[GetUser(event.Task, ssn)]
				nsOpt.allocated.Sub(event.Task.Resreq)

				drf.updateNamespaceShare(GetUser(event.Task, ssn), nsOpt)
				nsShare = nsOpt.share
			}

			klog.V(3).Infof("DRF EvictFunc: task <%v/%v>, resreq <%v>,  share <%v>, namespace share <%v>",
				event.Task.Namespace, event.Task.Name, event.Task.Resreq, attr.share, nsShare)
		},
	})
}

func (drf *drfPlugin) updateNamespaceShare(namespaceName string, attr *drfAttr) {
	drf.updateShare(attr)
	metrics.UpdateNamespaceShare(namespaceName, attr.share)
}

func (drf *drfPlugin) updateJobShare(jobNs, jobName string, attr *drfAttr) {
	drf.updateShare(attr)
	metrics.UpdateJobShare(jobNs, jobName, attr.share)
}

func (drf *drfPlugin) updateShare(attr *drfAttr) {
	attr.dominantResource, attr.share = drf.calculateShare(attr.allocated, drf.totalResource)
}

func (drf *drfPlugin) calculateShare(allocated, totalResource *api.Resource) (string, float64) {
	res := float64(0)
	dominantResource := ""
	for _, rn := range totalResource.ResourceNames() {
		share := helpers.Share(allocated.Get(rn), totalResource.Get(rn))
		if share > res {
			res = share
			dominantResource = string(rn)
		}
	}

	return dominantResource, res
}

func (drf *drfPlugin) OnSessionClose(session *framework.Session) {
	// Clean schedule data.
	drf.totalResource = api.EmptyResource()
	drf.jobAttrs = map[api.JobID]*drfAttr{}
}

func GetUser(ti *api.TaskInfo, ssn *framework.Session) string {
	return ssn.Jobs[ti.Job].GetUser()
}
