# 003: ingress scope

Stage: S0 · Date: 2026-11-21 · Author: anand24mishra · Drafted by agent: yes

## Choice

Restrict port 80 HTTP ingress on the app host to the student's laptop public IP (`115.244.141.202/32`) and the instructor's grader IP list in `terraform.tfvars`.

## Rejected alternative

Opening port 80 to the public Internet (`0.0.0.0/0`).

## Why

The app host runs an unauthenticated Traefik proxy and stub FastAPI backend. Opening it to `0.0.0.0/0` invites continuous port scanning, automated bot vulnerability probes, and potential abuse on limited Learner Lab EC2 resources. Restricting to known CIDRs limits attack surface and eliminates unsolicited traffic.

## What would show this was wrong

If frequently switching between networks (e.g. mobile hotspot or dynamic ISP) causes frequent IP changes that disrupt development and require constant `terraform.tfvars` updates.
