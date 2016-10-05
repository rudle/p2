package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/util"
	"gopkg.in/alecthomas/kingpin.v2"
	klabels "k8s.io/kubernetes/pkg/labels"
)

var (
	labelType         = kingpin.Flag("labelType", "The type of label to adjust. Sometimes called the \"label tree\"").Short('t').Required().String()
	subjectSelector   = kingpin.Flag("selector", "The selector on which to modify labels.").Short('s').Required().String()
	addititiveLabels  = kingpin.Flag("add", "The label set to apply to the subject.").Short('a').String()
	destructiveLabels = kingpin.Flag("delete", "The label set to remove to the subject. It is not an error to include extra labels here.").Short('d').String()
)

func main() {
	_, opts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	_ = client
	store := kp.NewConsulStore(client)
	_ = store

	subject, err := klabels.Parse(*subjectSelector)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while parsing subject label. Check the syntax. \n%v\n", err)
	}

	labelType, err := labels.AsType(*labelType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while parsing label type. Check the commandline. \n%v\n", err)
	}

	applicator := labels.NewConsulApplicator(client, 3)

	additive, err := klabels.Parse(*addititiveLabels)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while parsing additive label set. Check the syntax. \n%v\n", err)
	}
	destructive, err := klabels.Parse(*destructiveLabels)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while parsing destructive label set. Check the syntax. \n%v\n", err)
	}

	cachedMatch := false
	matches, err := applicator.GetMatches(subject, labelType, cachedMatch)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error while finding label matches. Syntax error? \n%v\n", err)
	}

	for _, match := range matches {
		entityID := match.ID

		applyLabels(applicator, entityID, labelType, additive, destructive)
	}
}

func applyLabels(applicator labels.Applicator, entityID string, labelType labels.Type, additiveLabels, destructiveLabels klabels.Selector) error {
	var err error

	labelsForEntity, err := applicator.GetLabels(labelType, entityID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Got error while querying labels. %v\n", err)
		return err
	}
	fmt.Printf("The current labels are: %s\n", labelsForEntity.Labels.String())
	if !additiveLabels.Empty() {
		fmt.Printf("labels to be added: %s\n", additiveLabels.String())
	}

	if !destructiveLabels.Empty() {
		fmt.Printf("labels to be removed: %s\n", destructiveLabels.String())
	}
	fmt.Println("Continue?")
	if !confirm() {
		return util.Errorf("Operation canceled")
	}

	if !additiveLabels.Empty() {
		for _, label := range strings.Split(additiveLabels.String(), ",") {
			kvs := strings.Split(label, "=")
			if len(kvs) != 2 {
				return util.Errorf("Unexpected non-binary label")
			}
			err = applicator.SetLabel(labelType, entityID, kvs[0], kvs[1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error while appyling label. k/v: %s/%s. \n%v\n", kvs[0], kvs[1], err)
			}
		}
	}
	if !destructiveLabels.Empty() {
		for _, label := range strings.Split(destructiveLabels.String(), ",") {
			kvs := strings.Split(label, "=")
			if len(kvs) != 2 {
				return util.Errorf("Unexpected non-binary label")
			}
			applicator.RemoveLabel(labelType, entityID, kvs[0])
		}
	}

	return nil
}

func confirm() bool {
	fmt.Printf(`Type "y" to confirm [n]: `)
	var input string
	_, err := fmt.Scanln(&input)
	if err != nil {
		return false
	}
	resp := strings.TrimSpace(strings.ToLower(input))
	return resp == "y" || resp == "yes"
}
