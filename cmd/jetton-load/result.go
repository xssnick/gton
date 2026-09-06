package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

const resultSchemaVersion = 1

const (
	resultOutcomeComplete           = "complete"
	resultOutcomeDeliveryIncomplete = "delivery_incomplete"
	resultOutcomeExecutionFailed    = "execution_failed"
	resultOutcomeSubmissionFailed   = "submission_failed"
	resultOutcomeWorkloadInvalid    = "workload_incompatible"
	resultOutcomeFailed             = "failed"
)

type commandResult struct {
	SchemaVersion           int     `json:"schema_version"`
	Command                 string  `json:"command"`
	Outcome                 string  `json:"outcome"`
	Error                   string  `json:"error,omitempty"`
	FailureStage            string  `json:"failure_stage,omitempty"`
	ContractProfile         string  `json:"contract_profile,omitempty"`
	MinterCodeHash          string  `json:"minter_code_hash,omitempty"`
	WalletCodeHash          string  `json:"wallet_code_hash,omitempty"`
	SenderIndex             uint64  `json:"sender_index"`
	Submitted               int     `json:"submitted"`
	Accepted                int     `json:"accepted"`
	FailedBatches           int     `json:"failed_batches"`
	ExternalBatches         int     `json:"external_batches"`
	RPCAcceptedBatches      int     `json:"rpc_accepted_batches"`
	CanarySubmitted         int     `json:"canary_submitted"`
	CanaryAccepted          int     `json:"canary_accepted"`
	Undelivered             int     `json:"undelivered"`
	SubmittedTPS            float64 `json:"submitted_tps"`
	SourceBalanceBefore     string  `json:"source_balance_before,omitempty"`
	SourceBalanceAfter      string  `json:"source_balance_after,omitempty"`
	SourceBalanceCurrent    string  `json:"source_balance_current,omitempty"`
	RecipientBalanceBefore  string  `json:"recipient_balance_before,omitempty"`
	RecipientBalanceAfter   string  `json:"recipient_balance_after,omitempty"`
	RecipientBalanceCurrent string  `json:"recipient_balance_current,omitempty"`
	RunEpoch                string  `json:"run_epoch,omitempty"`
	Minter                  string  `json:"minter,omitempty"`
	HighloadWallet          string  `json:"highload_wallet,omitempty"`
	SourceJettonWallet      string  `json:"source_jetton_wallet,omitempty"`
}

func newCommandResult(command string, senderIndex uint64) commandResult {
	return commandResult{
		SchemaVersion: resultSchemaVersion,
		Command:       command,
		Outcome:       resultOutcomeComplete,
		SenderIndex:   senderIndex,
	}
}

func finishCommand(result commandResult, commandErr error) error {
	if normalizeErr := normalizeRunCounters(&result); normalizeErr != nil {
		commandErr = errors.Join(commandErr, normalizeErr)
		result.Outcome = resultOutcomeFailed
		result.FailureStage = "result"
	}

	if commandErr != nil {
		if result.Outcome == resultOutcomeComplete {
			result.Outcome = resultOutcomeFailed
		}
		if result.FailureStage == "" {
			result.FailureStage = "command"
		}
		result.Error = commandErr.Error()
	}

	encodeErr := writeCommandResult(os.Stdout, result)
	if encodeErr != nil {
		encodeErr = fmt.Errorf("write command result: %w", encodeErr)
	}

	return errors.Join(commandErr, encodeErr)
}

func normalizeRunCounters(result *commandResult) error {
	if result.Command != "run" {
		return nil
	}
	if result.Submitted < 0 {
		return fmt.Errorf("submitted transfer count is negative: %d", result.Submitted)
	}
	if result.Accepted < 0 {
		return fmt.Errorf("accepted transfer count is negative: %d", result.Accepted)
	}
	if result.Accepted > result.Submitted {
		return fmt.Errorf("accepted transfer count %d exceeds submitted count %d", result.Accepted, result.Submitted)
	}

	result.Undelivered = result.Submitted - result.Accepted
	return nil
}

func writeCommandResult(output io.Writer, result commandResult) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}
