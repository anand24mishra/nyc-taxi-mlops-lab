# 001: runner credentials

Stage: S0 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

The self-hosted GitHub Actions runner uses the EC2 host's instance profile (`LabInstanceProfile`) rather than static AWS session credentials stored in GitHub Secrets.

## Rejected alternative

Storing temporary Learner Lab credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) as GitHub repository secrets.

## Why

In AWS Academy Learner Lab, session tokens expire every 4 hours. Using static session credentials would break CD deploys every 4 hours and require manual re-exporting and pasting. The instance profile is automatically refreshed by the AWS Instance Metadata Service (IMDSv2) on the host, requires zero secrets to manage, and eliminates credential leakage risks in public repositories.

## What would show this was wrong

If GitHub Actions needed to execute AWS deployment calls from GitHub-hosted runners rather than the self-hosted EC2 runner, or if tasks required IAM permissions outside `LabRole`.
