# Download and install LLVM
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 16
rm ./llvm.sh

# Update package lists
sudo apt update

# Install required packages
sudo apt install -y build-essential libtbb-dev librocksdb-dev libhiredis-dev libaio-dev libconfig-dev libxxhash-dev libssl-dev libnuma-dev libjemalloc-dev clang-format-16 python3 python3-pip htop linux-tools-$(uname -r)

# Ensure clang is linked correctly
if [ ! -f /usr/bin/clang ]; then
    sudo ln -s /usr/bin/clang-16 /usr/bin/clang
fi

# Install Python packages
python3 -mpip install pandas

# Clone the SplinterDB repository
git clone https://github.com/vmware/splinterdb.git ../splinterdb

# Setup CPU governor
if [[ $(lscpu | grep "Model name" | grep -c "Intel") -eq 1 ]]; then
    echo "performance" | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
fi

# Add me to group disk
sudo usermod -a -G disk $USER
echo "Please log out and log back in for the changes to take effect."