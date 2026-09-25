#!/bin/bash
if [[ -z "${CONTROL_PLANE_IP}" ]]; then
  echo "No control plane set"    
else
  sed -i "s/CONTROL_PLANE_IP/$CONTROL_PLANE_IP/g" ./arion-config.yaml    
fi
