#!/bin/bash

# Leadfinder Backend EC2 Deployment Script
# Make sure to run this script from your local machine, not on EC2

echo "🚀 Starting Leadfinder Backend Deployment to EC2..."

# Configuration - UPDATE THESE VALUES
EC2_HOST="ec2-52-91-22-229.compute-1.amazonaws.com"
EC2_USER="ubuntu"
EC2_KEY_PATH="./exellius-pem-1.pem"
PROJECT_NAME="leadfinder"
REMOTE_DIR="/home/ubuntu"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

echo -e "${YELLOW}📋 Configuration:${NC}"
echo "EC2 Host: $EC2_HOST"
echo "EC2 User: $EC2_USER"
echo "Project: $PROJECT_NAME"
echo "Remote Directory: $REMOTE_DIR/$PROJECT_NAME"
echo ""

# Check if required files exist
if [ ! -f "package.json" ]; then
    print_error "package.json not found in current directory"
    exit 1
fi

if [ ! -f "server.js" ]; then
    print_error "server.js not found in current directory"
    exit 1
fi

if [ ! -f "$EC2_KEY_PATH" ]; then
    print_error "PEM file not found: $EC2_KEY_PATH"
    exit 1
fi

if [ ! -f ".env" ]; then
    print_error ".env file not found in current directory - this is required for deployment"
    exit 1
fi

print_status "Local files verified"

# Set correct permissions on PEM file
chmod 400 $EC2_KEY_PATH

# Test SSH connection
print_info "Testing SSH connection..."
if ssh -i $EC2_KEY_PATH -o ConnectTimeout=10 -o BatchMode=yes $EC2_USER@$EC2_HOST exit 2>/dev/null; then
    print_status "SSH connection successful"
else
    print_error "Failed to connect to server"
    exit 1
fi

# Create deployment package
print_info "Creating deployment package..."
tar --exclude='node_modules' \
    --exclude='.git' \
    --exclude='logs' \
    --exclude='*.log' \
    --exclude='deploy.tar.gz' \
    --exclude='.gitignore' \
    -czf deploy.tar.gz .

print_status "Deployment package created"

# Upload to EC2
print_info "Uploading to EC2..."
scp -i $EC2_KEY_PATH deploy.tar.gz $EC2_USER@$EC2_HOST:~/

print_status "Files uploaded to server"

# Execute deployment commands on EC2
print_info "Executing deployment on EC2..."
ssh -i $EC2_KEY_PATH $EC2_USER@$EC2_HOST << 'ENDSSH'
    # Colors for remote output
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m' # No Color
    
    print_status() {
        echo -e "${GREEN}✅ $1${NC}"
    }
    
    print_info() {
        echo -e "${BLUE}ℹ️  $1${NC}"
    }
    
    print_warning() {
        echo -e "${YELLOW}⚠️  $1${NC}"
    }
    
    PROJECT_NAME="leadfinder"
    REMOTE_DIR="/home/ubuntu"
    BACKUP_DIR="/home/ubuntu/backups"
    
    # Create backup of current deployment
    if [ -d "$REMOTE_DIR/$PROJECT_NAME" ]; then
        print_info "Creating backup of current deployment..."
        mkdir -p $BACKUP_DIR
        TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
        BACKUP_NAME="leadfinder_backup_$TIMESTAMP"
        
        # Copy current application (excluding node_modules)
        rsync -av --exclude='node_modules' --exclude='logs' --exclude='*.log' \
              $REMOTE_DIR/$PROJECT_NAME/ $BACKUP_DIR/$BACKUP_NAME/
        
        print_status "Backup created: $BACKUP_DIR/$BACKUP_NAME"
        
        # Keep only last 5 backups
        cd $BACKUP_DIR
        ls -t | grep "leadfinder_backup_" | tail -n +6 | xargs -r rm -rf
    fi
    
    # Stop existing application if running
    if pm2 list | grep -q "leadfinder"; then
        print_info "Stopping existing application..."
        pm2 stop leadfinder
    fi
    
    # Remove old deployment files (but keep uploads)
    if [ -d "$REMOTE_DIR/$PROJECT_NAME" ]; then
        print_info "Preserving uploads directory..."
        cp -r $REMOTE_DIR/$PROJECT_NAME/uploads /tmp/uploads.backup 2>/dev/null || true
    fi
    
    # Remove old files
    rm -rf $REMOTE_DIR/$PROJECT_NAME
    
    # Extract new deployment
    print_info "Extracting new deployment..."
    mkdir -p $REMOTE_DIR/$PROJECT_NAME
    tar -xzf ~/deploy.tar.gz -C $REMOTE_DIR/$PROJECT_NAME
    cd $REMOTE_DIR/$PROJECT_NAME
    
    # Always use the .env file from the deployment package
    print_status "Using .env file from deployment package"
    
    if [ -d "/tmp/uploads.backup" ]; then
        cp -r /tmp/uploads.backup uploads
        rm -rf /tmp/uploads.backup
        print_status "Uploads directory restored"
    else
        mkdir -p uploads
    fi
    
    # Install dependencies
    print_info "Installing dependencies..."
    npm ci --production
    
    # Create logs directory
    mkdir -p logs
    
    # Update environment for production
    if [ -f ".env" ]; then
        print_info "Updating URLs for production environment..."
        # Update URLs for production if they contain localhost
        sed -i "s|http://localhost:5000|https://api.app.exellius.com|g" .env
        sed -i "s|GOOGLE_CALLBACK_URL=http://localhost:5000|GOOGLE_CALLBACK_URL=https://api.app.exellius.com|g" .env
        print_status "Environment file configured for production"
    else
        print_error "No .env file found after deployment!"
        exit 1
    fi
    
    # Start application with PM2
    print_info "Starting application..."
    pm2 start ecosystem.config.cjs --env production
    
    # Save PM2 configuration
    pm2 save
    
    # Test if application is responding
    sleep 3
    if curl -f -s http://localhost:5000 > /dev/null; then
        print_status "Application is responding"
    else
        print_warning "Application might not be responding - check logs"
    fi
    
    # Reload nginx configuration
    if systemctl is-active --quiet nginx; then
        print_info "Reloading nginx configuration..."
        sudo nginx -t && sudo systemctl reload nginx
        print_status "Nginx reloaded"
    fi
    
    # Clean up
    rm ~/deploy.tar.gz
    
    print_status "Deployment completed successfully!"
    echo ""
    print_info "Application status:"
    pm2 list
    
    echo ""
    print_info "Recent logs (last 10 lines):"
    pm2 logs leadfinder --lines 10 --nostream
ENDSSH

# Clean up local deployment package
rm deploy.tar.gz

print_status "🎉 Deployment completed!"
echo ""
print_info "📝 Your application is now running at:"
echo "  • HTTP: http://$EC2_HOST"
echo "  • API: http://$EC2_HOST/api/auth/login"
echo ""
print_info "📊 To check status, run:"
echo "  ssh -i $EC2_KEY_PATH $EC2_USER@$EC2_HOST 'pm2 status'"
echo ""
print_info "📋 To view logs, run:"
echo "  ssh -i $EC2_KEY_PATH $EC2_USER@$EC2_HOST 'pm2 logs leadfinder'"
echo ""
print_warning "🌐 For custom domain (api.app.exellius.com):"
echo "  1. Point DNS A record to: $(ssh -i $EC2_KEY_PATH $EC2_USER@$EC2_HOST 'curl -s ifconfig.me')"
echo "  2. Run: ssh -i $EC2_KEY_PATH $EC2_USER@$EC2_HOST 'sudo certbot --nginx -d api.app.exellius.com'" 