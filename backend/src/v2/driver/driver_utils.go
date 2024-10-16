package driver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/api/v2alpha1/go/pipelinespec"
	"github.com/kubeflow/pipelines/backend/src/v2/metadata"
)

type ArtifactReader interface {
	GetOutputArtifactsByExecutionId(ctx context.Context, executionId int64) (map[string]*metadata.OutputArtifact, error)
}

func resolveUpstreamArtifacts(ctx context.Context, tasks map[string]*metadata.Execution, taskName string, outputArtifactKey string, mlmd ArtifactReader) (runtimeArtifact *pipelinespec.RuntimeArtifact, err error) {
	glog.V(4).Info("taskName: ", taskName)
	glog.V(4).Info("outputArtifactKey: ", outputArtifactKey)
	upstreamTask := tasks[taskName]
	if *upstreamTask.GetExecution().Type == "system.DAGExecution" {
		// recurse
		outputArtifactsCustomProperty, ok := upstreamTask.GetExecution().GetCustomProperties()["output_artifacts"]
		if !ok {
			return nil, fmt.Errorf("cannot find output_artifacts")
		}
		var outputArtifacts map[string]*pipelinespec.DagOutputsSpec_DagOutputArtifactSpec
		glog.V(4).Infof("outputArtifactsCustomProperty: %#v", outputArtifactsCustomProperty)
		glog.V(4).Info("outputArtifactsCustomProperty String: ", outputArtifactsCustomProperty.GetStringValue())
		err = json.Unmarshal([]byte(outputArtifactsCustomProperty.GetStringValue()), &outputArtifacts)
		if err != nil {
			return nil, err
		}
		glog.V(4).Info("Deserialized outputArtifactsMap: ", outputArtifacts)
		var subTaskName string
		outputArtifactSelectors := outputArtifacts[outputArtifactKey].GetArtifactSelectors()
		for _, outputArtifactSelector := range outputArtifactSelectors {
			subTaskName = outputArtifactSelector.ProducerSubtask
			outputArtifactKey = outputArtifactSelector.OutputArtifactKey
			glog.V(4).Infof("ProducerSubtask: %v", outputArtifactSelector.ProducerSubtask)
			glog.V(4).Infof("OutputArtifactKey: %v", outputArtifactSelector.OutputArtifactKey)
		}
		downstreamParameterMapping, err := resolveUpstreamArtifacts(ctx, tasks, subTaskName, outputArtifactKey, mlmd)
		glog.V(4).Infof("downstreamParameterMapping: %#v", downstreamParameterMapping)
		if err != nil {
			return nil, err
		}
		return downstreamParameterMapping, nil
	} else {
		// base case
		outputs, err := mlmd.GetOutputArtifactsByExecutionId(ctx, upstreamTask.GetID())
		if err != nil {
			return nil, err
		}
		artifact, ok := outputs[outputArtifactKey]
		if !ok {
			return nil, fmt.Errorf(
				"cannot find output artifact key %q in producer task %q",
				outputArtifactKey,
				taskName,
			)
		}
		runtimeArtifact, err := artifact.ToRuntimeArtifact()
		if err != nil {
			return nil, err
		}
		return runtimeArtifact, nil
	}
}
