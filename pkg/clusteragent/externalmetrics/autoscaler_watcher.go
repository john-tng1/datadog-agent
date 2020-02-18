// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

// +build kubeapiserver

package externalmetrics

import (
	"fmt"
	"time"

	"github.com/DataDog/datadog-agent/pkg/util/kubernetes/apiserver"
	"github.com/DataDog/datadog-agent/pkg/util/log"

	dd_clientset "github.com/DataDog/datadog-operator/pkg/generated/clientset/versioned"
	dd_informers "github.com/DataDog/datadog-operator/pkg/generated/informers/externalversions"
	dd_listers "github.com/DataDog/datadog-operator/pkg/generated/listers/datadoghq/v1alpha1"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// DatadogMetricController watches DatadogMetric to build an internal view of current DatadogMetric state.
// * It allows any ClusterAgent (even non leader) to answer quickly to Autoscalers queries
// * It allows leader to know the list queries to send to DD
type DatadogMetricController struct {
	clientSet dd_clientset.Interface
	lister    dd_listers.DatadogMetricLister
	le        apiserver.LeaderElectorInterface
}

// NewAutoscalersController returns a new AutoscalersController
func NewDatadogMetricController(resyncPeriod int64, client dd_clientset.Interface, informer dd_informers.SharedInformerFactory, le apiserver.LeaderElectorInterface, store *DatadogMetricsInternalStore) (*DatadogMetricController, error) {
	if store == nil {
		return nil, fmt.Errorf("Store cannot be nil")
	}

	datadogMetricsInformer := informer.Datadoghq().V1alpha1().DatadogMetrics()
	c := &DatadogMetricController{
		clientSet: client,
		lister:    datadogMetricsInformer.Lister(),
		synced:    datadogMetricsInformer.Informer().HasSynced,
		workqueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultItemBasedRateLimiter(), "datadogmetrics"),
		store:     store,
		le:        le,
	}

	// We use resync to sync back information to DatadogMetrics
	datadogMetricsInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueue,
		DeleteFunc: c.enqueue,
		UpdateFunc: func(obj, new interface{}) {
			c.enqueue(new)
		},
	}, time.Duration(resyncPeriod)*time.Second)

	return c, nil
}

// Run starts the controller to handle DatadogMetrics
func (c *DatadogMetricController) Run(stopCh <-chan struct{}) error {
	defer c.workqueue.ShutDown()

	log.Infof("Starting DatadogMetric Controller... ")
	if !cache.WaitForCacheSync(stopCh, c.synced) {
		return fmt.Errorf("Failed to wait for DatadogMetric caches to sync")
	}

	go wait.Until(c.worker, time.Second, stopCh)

	log.Infof("Started DatadogMetric Controller")
	<-stopCh
	log.Infof("Stopping DatadogMetric Controller")
	return nil
}
