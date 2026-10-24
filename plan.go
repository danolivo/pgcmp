package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// planNode is the part of EXPLAIN (FORMAT JSON) output we look at.
type planNode struct {
	NodeType      string     `json:"Node Type"`
	ParallelAware bool       `json:"Parallel Aware"`
	Plans         []planNode `json:"Plans"`
}

// planShape returns the node types of the plan from the top down, e.g.
// "Gather Merge -> Sort -> Parallel Seq Scan", and whether it contains a
// (parallel) sequential scan. With ORDER BY on the key, a sequential scan
// means the whole table is read and then sorted. Other sorts, such as a
// bitmap index scan of one key range followed by a sort of that range, do
// not read the whole table and are not reported.
func planShape(n planNode) (shape string, seqScan bool) {
	var names []string
	var walk func(n planNode)
	walk = func(n planNode) {
		name := n.NodeType
		if n.ParallelAware {
			name = "Parallel " + name
		}
		names = append(names, name)
		if n.NodeType == "Seq Scan" {
			seqScan = true
		}
		for _, c := range n.Plans {
			walk(c)
		}
	}
	walk(n)
	return strings.Join(names, " -> "), seqScan
}

// explain plans the query on this side without running it.
func (s *side) explain(ctx context.Context, sql string) (planNode, error) {
	var out string
	if err := s.tx.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql).Scan(&out); err != nil {
		return planNode{}, fmt.Errorf("%s: explain: %w", s.name, err)
	}
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(out), &plans); err != nil || len(plans) == 0 {
		return planNode{}, fmt.Errorf("%s: explain: unexpected output: %v", s.name, err)
	}
	return plans[0].Plan, nil
}

// checkPlans explains every range query on this side and returns a warning
// if any of them uses a sequential scan, or "" otherwise.
func (s *side) checkPlans(ctx context.Context, table string, queries []string) (string, error) {
	seq, example := 0, ""
	for _, q := range queries {
		plan, err := s.explain(ctx, q)
		if err != nil {
			return "", err
		}
		if shape, ok := planShape(plan); ok {
			seq++
			if example == "" {
				example = shape
			}
		}
	}
	switch {
	case seq == 0:
		return "", nil
	case len(queries) == 1:
		return fmt.Sprintf("warning: %s: %s will be read with a sequential scan and sorted in full (%s) "+
			"instead of through the primary key index; this may be slow and need a lot of temporary space",
			s.name, table, example), nil
	default:
		return fmt.Sprintf("warning: %s: %d of %d key range queries on %s use a sequential scan (%s): "+
			"each of them reads the whole table and sorts its range",
			s.name, seq, len(queries), table, example), nil
	}
}
