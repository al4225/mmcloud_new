# Minimal VM image for mmcloud

This image is designed to work for mmcloud as the base image where we add additional package on the fly. Instead of the typical approach of installing software into a container, we install our software into an AWS FSx volume with pixi and mount this volume into the container at run time.  This greatly reduces the startup time for jobs as well as the EBS volume storage requirements for the host VM.

To use this image in mmcloud please check this page: https://wanggroup.org/productivity_tips/mmcloud-interactive
