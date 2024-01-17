sudo docker build -t webhook .
sudo docker run -d -p 8008:8008 --env-file .env webhook

sudo docker logs <containerID>
sudo docker logs -f --tail 20 <containerID>
sudo docker exec -it <containerID> /bin/bash
sudo docker cp <containerID>:<path> .



sudo docker system prune -a
sudo docker image prune -a
sudo docker container prune
sudo docker volume prune
sudo docker network prune
df -h


sudo apt autoremove
sudo apt clean
sudo dpkg --list 'linux-image*'
sudo apt-get purge <old-kernel-package>
sudo du -h /var/log
sudo rm /var/log/<large-log-file>
sudo du -h --max-depth=1 /
sudo du -h --max-depth=1 /var/lib/docker
sudo find / -type f -size +1G
sudo docker system prune -a
sudo find / -name "core" -exec rm -f {} \;


ssh -i ~/Downloads/dev-iot_key.pem PipelineSecret@4.194.114.42
