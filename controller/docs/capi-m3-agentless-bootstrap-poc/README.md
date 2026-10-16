# CAPI M3 Agent-Less Bootstrap POC Templates

These templates are the MachineDeployment test manifests used to prove the two
agent-less CABPK worker bootstrap paths described in
[`../capi_m3_agentless_bootstrap_design.md`](../capi_m3_agentless_bootstrap_design.md).

They are POC scaffolding, not production manifests. The production
implementation should render equivalent resources from NVKE controller code and
release data.

## Templates

- `machinedeployment-imds.yaml.tmpl` creates a CAPNICo-backed worker
  `MachineDeployment` whose CABPK discovery kubeconfig uses the
  `imds-jwt-exec-auth` exec credential plugin.
- `machinedeployment-spire.yaml.tmpl` creates a CAPNICo-backed worker
  `MachineDeployment` whose CABPK discovery kubeconfig uses the SPIFFE JWT exec
  credential plugin and the temporary `kubeadm/join/<machine-uid>` SPIRE entry
  flow.

## Usage

Render a template with environment substitution after setting the variables in
the template:

```bash
envsubst < machinedeployment-imds.yaml.tmpl > /tmp/machinedeployment-imds.yaml
envsubst < machinedeployment-spire.yaml.tmpl > /tmp/machinedeployment-spire.yaml
```

The templates assume:

- A CAPI cluster already exists in `nke-system`.
- CABPK prerequisites are already published: `<cluster>-ca` and
  `Cluster.spec.controlPlaneEndpoint`.
- The controller is configured for the matching worker bootstrap identity source.
- Required POC artifacts are reachable from the worker during cloud-init.

Do not put real credentials, tokens, kubeconfigs, or certificates in these
templates. Use placeholders or externally managed test secrets.
