# Sync WSL data and files back to Windows
Write-Host "Syncing WSL output & data to Windows..." -ForegroundColor Cyan
wsl -d Ubuntu-24.04 -u raj bash -c "rsync -rtvu --no-perms --no-owner --no-group --exclude='.git' --exclude='__pycache__' /home/raj/BDA_Project/Stock-Analysis/ /mnt/c/Users/Raj/OneDrive/Desktop/Stock-Analysis/Stock-Analysis/"
Write-Host "Sync from WSL completed successfully!" -ForegroundColor Green
