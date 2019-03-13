<h1>Optimizing Data-Intensive Visualization Pipelines using Pixel-Perfect Compression</h1>
<h2>GROUP 11</h2>

| Member            | Matriculum No.| GitHub  |
| :---------------- |--------------:| :-------|
| Christoph Schulze | 389866        | @ch-sc   |
| Laura Mons        | 364309        | @eleicha |

    Supervisor:
     Sebastian Breß
     @sebastianbress

<h2>Project Summary</h2>
<p>
Enabling quick and easy interpretations of vast amounts of data collected on a daily basis is a 
key challenge for any data analyst. In this context, data visualizations have proven themselves 
as an invaluable tool which is widely used in industry and research.
</p>
<p>
Data-intensive visualizations do not scale well for large amounts of data. The data transfer from 
a data processing system to a visualization system is the primary bottleneck. Consequently, the 
transferred data volume needs to be reduced. One way to achieve this would be by pixel-perfect 
compression.
</p>
<p>
Especially important in this context is the efficient visualization of constantly incoming data 
(streamed data) created by devices such as sensors. In order to prevent a visualization component 
from performing computational intensive calculations which ultimately will lead to display latency,
it is beneficial to reduce data beforehand in a streaming processing device. We evaluate the 
application of Visualization-Driven Data Aggregation (VDDA) in combination with common clustering 
algo-rithms to data streams using Flink as stream processing component. We found that applying 
VDDA will speed up computations as well as reduce data. However, applying a clustering algorithm 
such as k-means to the data afterwards will yield different results since VDDA changes the 
underlying distribution.
</p>


<h2>Reproducing the Experiment</h2>
Clone this repository, and follow the instructions of this section to run the experiment on your own.

<h3>Maven</h3>
To build the sources you can use Maven and run
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


<h3>Do A Test Run</h3>
Go into your browser and call:
```
http://localhost:8082/2d/stream/cluster?x=3840&y=2160&opt=VDDA
```
If everything is wired up correctly the request may take a while to finish and should eventually see messages in the 
Queue. If you exposed the port for RabbitMQ's admin page (8090:15672) you can check whether there are messages in the queue.
```
http://localhost:8090/#/queues/%2F/BDAPRO2
```
