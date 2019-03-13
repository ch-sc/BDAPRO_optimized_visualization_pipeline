<h1>Optimizing Data-Intensive Visualization Pipelines using Pixel-Perfect Compression</h1>
<h2>GROUP 11</h2>

| Member            | Matriculum No.| GitHub                                 |
| :---------------- |--------------:| :--------------------------------------|
| Christoph Schulze | 389866        | [@ch-sc](https://github.com/ch-sc)     |
| Laura Mons        | 364309        | [@eleicha](https://github.com/eleicha) |

    Supervisor:
     Sebastian Breß
     @sebastianbress (https://github.com/sebastianbress)
     

<h2>Description</h2>
<p>
Enabling quick and easy interpretations of vast amounts of data collected on a daily basis is a key challenge for any data analyst. In this context, data visualizations have proven themselves as an invaluable tool which is widely used in industry and research.
</p>
<p>
Data-intensive visualizations do not scale well for large amounts of data. The data transfer from a data processing system to a visualization system is the primary bottleneck. Consequently, the transferred data volume needs to be reduced. One way to achieve this would be by pixel-perfect compression, however, other ways are possible as well.
</p>
<p>
Existing approaches such as M4 [1] are already applying pixel-perfect-compression, e.g. for line charts. Following their lead, the scope of this project is to extend prior work by a richer set of visualizations, such as scatter plots, bar charts, or space filling visualizations. The extensions should be able to prevent system crashes as well as enable quick data transfers to the visualization component. At the same time, all relevant data should be displayed.
</p>

<h2>Reproducing the Experiment</h2>
Clone this repository and follow the instructions of this section to run the experiment on your own.

<h3>Acquiring the Data</h3>
For our experiment we used the yellow taxi data set provided by the New York Taxi & Limousine 
Commission. You can download it here: 
[TLC Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

<h3>Maven</h3>
To build the sources you can use Maven
```
mvn compile
```

<h3>RabbitMQ</h3>
Pull and start the 'rabbitmq:3-management' docker image (EXPOSE 5672:5672, 8090:15672). By default username and password
are set to "user" and "password". You may need to enter these credentials when accessing the admin UI of RabbitMQ.

```
docker run -d --name rabbitmq --hostname my-rabbit -p 5672:5672 -p 8090:15672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password rabbitmq:3-management
```

<h3>Jupyter Notebook</h3>
Go into the notebooks directory of this repository and start Juptyer notebook. You should see two different notebooks. 
Open Notebook 'plots' and run all cells.

```
cd notebooks
jupyter notebook
```


<h3>Do a Test Run</h3>
Go into your browser and call:
```
http://localhost:8082/2d/stream/cluster?x=3840&y=2160&opt=VDDA
```
If everything is wired up correctly the request may take a while to finish and should eventually see messages in the 
Queue. If you exposed the port for RabbitMQ's admin page (8090:15672) you can check whether there are messages in the queue.
```
http://localhost:8090/#/queues/%2F/BDAPRO2