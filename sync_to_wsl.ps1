# Sync local Windows changes to Ubuntu WSL
Write-Host "Syncing Windows files to Ubuntu WSL..." -ForegroundColor Cyan
wsl -d Ubuntu-24.04 -u raj bash -c "rsync -rtv --no-perms --no-owner --no-group --exclude='.git' --exclude='__pycache__' /mnt/c/Users/Raj/OneDrive/Desktop/Stock-Analysis/Stock-Analysis/ /home/raj/BDA_Project/Stock-Analysis/"
Write-Host "Sync completed successfully!" -ForegroundColor Green
