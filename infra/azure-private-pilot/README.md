# Private T3 Code pilot on Azure

This pilot runs one Ubuntu 24.04 x64 VM in the DevOps subscription. It uses
Azure's default outbound access for the T3 Connect relay, package downloads,
and provider APIs. The NIC has only a private IP, the network security group
denies all inbound connections, and T3 listens on loopback. Use Azure VM Run
Command for maintenance and Azure Serial Console for interactive sign-in.

The 32 GiB Standard SSD data disk mounts at `/srv/t3`. T3 state and provider
credentials live under `/srv/t3/home`. Add projects under `/srv/t3/workspaces`.
The systemd service depends on that mount. Azure still bills for disks after
deallocation.

## Validate and deploy

Work from the repository root. Use `az` with Azure DevOps project
`Instablinds/t3code` for artifact reads and `az-admin` for the Azure
subscription, where the pilot's compute and network permissions live. You
need a local RSA SSH public key for the Azure Ubuntu image. The network blocks
SSH and the installer disables the guest SSH service.

```bash
rg=rg-t3-private-pilot-canadacentral
vm=t3-private-pilot
location=canadacentral
public_key="$HOME/.ssh/id_rsa.pub"

az devops project show --org https://dev.azure.com/Instablinds --project t3code --query name -o tsv
az-admin account show --subscription DevOps --query '{name:name,state:state}' -o table
az-admin bicep build --file infra/azure-private-pilot/main.bicep --stdout >/dev/null
az-admin deployment sub what-if --subscription DevOps --location "$location" --name "$vm" \
  --template-file infra/azure-private-pilot/main.bicep --parameters sshPublicKey="$(cat "$public_key")"
```

If Azure reports that a provider is unregistered, register only the required
provider in `DevOps` and rerun the preview. The deployment should add only the
pilot resource group, VM, data disk, VNet, subnet, NIC, and network security
group. Azure creates the OS disk with the VM. Confirm those resources and the
VM's regional price before deploying.

```bash
az-admin deployment sub create --subscription DevOps --location "$location" --name "$vm" \
  --template-file infra/azure-private-pilot/main.bicep --parameters sshPublicKey="$(cat "$public_key")" \
  --query properties.provisioningState -o tsv
az-admin vm list-ip-addresses --subscription DevOps -g "$rg" -n "$vm" \
  --query '[0].virtualMachine.network' -o json
az-admin network nsg rule show --subscription DevOps -g "$rg" --nsg-name "$vm-nsg" -n deny-inbound \
  --query '{direction:direction,access:access,source:sourceAddressPrefix,destination:destinationAddressPrefix,protocol:protocol}' -o json
```

## Install and verify

Run the installer with Python 3.11 or later. It selects the latest successful
main run from pipeline 26 by default. To repeat an exact installation, add
`--run-id <run>` for a successful main run. The installer verifies the build
commit and both SHA-256 sums from `t3-linux-server`. It sends a short-lived
pipeline download URL as a protected Run Command parameter. The VM checks
the archive and metadata again before installing. Keep shell tracing and
Azure CLI debug output off while installing.

```bash
python3 infra/azure-private-pilot/install.py
az-admin vm run-command show --subscription DevOps -g "$rg" --vm-name "$vm" --name install-t3 \
  --instance-view --query '{provisioning:provisioningState,state:instanceView.executionState,exitCode:instanceView.exitCode,output:instanceView.output}' -o json
az-admin vm run-command invoke --subscription DevOps -g "$rg" -n "$vm" \
  --command-id RunShellScript --scripts @infra/azure-private-pilot/verify.sh \
  --query 'value[].message' -o tsv
```

Wait for both install states to report `Succeeded`, exit code 0, and
`Installation complete` before running the guest checks. Confirm the guest
output includes `Pilot guest checks passed`. If the download expires, rerun the installer while
the VM is running. The installer restarts T3, so wait for active work to finish
before running it again. Use the same installer for later main builds.

## Sign in and test persistence

1. Start the VM if it is deallocated. In the Azure portal, open the VM's
   **Serial console**. Set a console password through **Reset password** for
   the `azurepilot` account if needed. Keep the password and sign-in codes in
   the Azure console and provider pages.
2. Run `sudo -iu t3`, then run `t3 connect link --headless` and approve the
   displayed code on the T3 Connect account that owns the hosted slot.
   Run `claude auth login --claudeai` and `codex login --device-auth` as the
   same `t3` user and complete the provider sign-ins. Exit to `azurepilot`
   and run `sudo systemctl restart t3-private-pilot.service`.
3. Open the pilot from your T3 Connect account. Add a project under
   `/srv/t3/workspaces` and run a real agent task that creates a file there.
4. Deallocate and start the VM with the commands below. Rerun the guest
   checks, open the same environment, and verify that the task history and
   workspace file are still present. Deallocate the VM when you finish.

```bash
az-admin vm deallocate --subscription DevOps -g "$rg" -n "$vm"
az-admin vm start --subscription DevOps -g "$rg" -n "$vm"
```
