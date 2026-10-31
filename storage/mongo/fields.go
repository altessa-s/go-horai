// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

const (
	opAnd              = "$and"
	opCond             = "$cond"
	opEq               = "$eq"
	opIfNull           = "$ifNull"
	opLiteral          = "$literal"
	opLt               = "$lt"
	opSet              = "$set"
	fieldId            = "_id"
	fieldEndedAt       = "ended_at"
	fieldLastRunId     = "last_run_id"
	fieldNextRunAt     = "next_run_at"
	fieldRevision      = "revision"
	fieldRunLeaseId    = "run_lease_id"
	fieldRunLeaseUntil = "run_lease_until"
	fieldRunStartedAt  = "run_started_at"
	fieldStartedAt     = "started_at"
	fieldTaskId        = "task_id"
)

const fieldStatus = "status"
