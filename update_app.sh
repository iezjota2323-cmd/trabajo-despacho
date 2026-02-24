#!/bin/bash
# Script de actualización automática para Paniagua Palacios

# Navegar a la carpeta del proyecto
cd /home/luispaniagua/trabajo-despacho

# Obtener cambios de GitHub
git pull origin main

# Reiniciar el servicio de la aplicación
sudo systemctl restart suite_financiera

echo "Actualización completada: $(date)"
