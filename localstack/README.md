## Localstack 

if any new file is added to the container or something changes on `init.sh` please, start it with:  
    
    `docker-compose up --force-recreate --build`
     
instead of regular: 

    `docker-compose up`
    
if you need to delete old files just remove `data` folder