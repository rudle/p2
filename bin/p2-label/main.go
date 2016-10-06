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
	cmdApply               = kingpin.Command(CmdApply, "Apply label changes to all objects matching a selector")
	applyLabelType         = cmdApply.Flag("labelType", "The type of label to adjust. Sometimes called the \"label tree\".\n\tSupported types can be found here: https://godoc.org/github.com/square/p2/pkg/labels#pkg-constants").Short('t').Required().String()
	applySubjectSelector   = cmdApply.Flag("selector", "The selector on which to modify labels.").Short('s').Required().String()
	applyAddititiveLabels  = cmdApply.Flag("add", "The label set to apply to the subject. Include multiple --add switches to include multiple labels\nExample: p2-label --selector $selector --add foo=bar --add bar=baz\nIt's safe to mix --add with --delete though the results of this command are not transactional.").Short('a').StringMap()
	applyDestructiveLabels = cmdApply.Flag("delete", "The label set to remove from the subject. Include multiple --delete switches to include multiple labels\nExample: p2-label --selector $selector --delete foo=bar --delete bar=baz \nIt's safe to mix --add with --delete though the results of this command are not transactional.").Short('d').StringMap()

	cmdShow       = kingpin.Command(CmdShow, "Show labels that apply to a particular entity (type, ID)")
	showLabelType = cmdShow.Flag("labelType", "The type of label to adjust. Sometimes called the \"label tree\".\n Supported types can be found here: https://godoc.org/github.com/square/p2/pkg/labels#pkg-constants").Short('t').Required().String()
	showID        = cmdShow.Flag("id", "The ID of the entity to show labels for.").Short('i').Required().String()
)

const (
	CmdApply = "apply"
	CmdShow  = "show"
)

func main() {
	cmd, opts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(opts)
	applicator := labels.NewConsulApplicator(client, 3)
	exitCode := 0

	switch cmd {
	case CmdShow:
		labelType, err := labels.AsType(*showLabelType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while parsing label type. Check the commandline. \n%v\n", err)
			break
		}

		labelsForEntity, err := applicator.GetLabels(labelType, *showID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error while querying labels. %v\n", err)
			break
		}
		fmt.Printf("%s: %s\n", *showID, labelsForEntity.Labels.String())
		return
	case CmdApply:
		labelType, err := labels.AsType(*applyLabelType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unrecognized type %s. Check the commandline and documentation.\nhttps://godoc.org/github.com/square/p2/pkg/labels#pkg-constants\n", *applyLabelType)
			break
		}

		subject, err := klabels.Parse(*applySubjectSelector)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while parsing subject label. Check the syntax.\n%v\n", err)
			break
		}

		additive := klabels.Set(*applyAddititiveLabels)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while parsing additive label set. Check the syntax.\n%v\n", err)
			break
		}
		destructive := klabels.Set(*applyDestructiveLabels)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while parsing destructive label set. Check the syntax.\n%v\n", err)
			break
		}

		cachedMatch := false
		matches, err := applicator.GetMatches(subject, labelType, cachedMatch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while finding label matches. Check the syntax.\n%v\n", err)
			break
		}

		for _, match := range matches {
			entityID := match.ID

			err := applyLabels(applicator, entityID, labelType, additive.AsSelector(), destructive.AsSelector())
			if err != nil {
				fmt.Printf("Encountered err during labeling, %v", err)
				exitCode = 1
			}
		}
		break
	}

	os.Exit(exitCode)
}

func applyLabels(applicator labels.Applicator, entityID string, labelType labels.Type, additiveLabels, destructiveLabels klabels.Selector) error {
	var err error

	labelsForEntity, err := applicator.GetLabels(labelType, entityID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Got error while querying labels. %v\n", err)
		return err
	}
	fmt.Printf("The current labels for %s are: %s\n", entityID, labelsForEntity.Labels.String())
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
