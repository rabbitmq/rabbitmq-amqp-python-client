#/bin/bash
version=$1
# Regex pattern for the valid strings
regex="^v([0-9]+)\.([0-9]+)\.([0-9]+)(-(alpha|beta|rc)\.([0-9]+))?$"

if [[ $version =~ $regex ]]; then
    echo "Creating tag with: " $1
    git tag -a -s -u $2 -m "rabbitmq-amqp-python-client $1" $1 && git push && git push --tags
    echo "Tag created: " $1
else
    echo "Invalid version" $1
fi