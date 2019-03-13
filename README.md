<h1>Optimizing Data-Intensive Visualization Pipelines using Pixel-Perfect Compression</h1>
<h2>GROUP 11</h2>

| Member            | Matriculum No.| GitHub  |
| :---------------- |--------------:| :-------|
| Christoph Schulze | 389866        | @ch-sc   |
| Laura Mons        | 364309        | @eleicha |

    Supervisor:
     Sebastian Breß
     @sebastianbress

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
<h2>
Deliverables
</h2>

<ul> 
    <li>
       To get a good overview about the advancements of this project, a baseline for comparison is required. This could be done in Apache Flink/Spark. However, other baselines are also possible.
    </li>
    <li>
        The implementation of pixel-perfect-aggregation for selected visualizations not yet addressed by M4.
    </li>
    <li>
        Evaluation of performance of the visualization by comparing the baseline with optimized implementation using compression
    </li>
    <li>
        Compare compression-ratios and the time-to-plot for the optimized methods
    </li>
    <li>
        Analyze the performance relation of the compression-ratio and the time to plot data
    </li>
</ul>
<h2>
Required Skills
</h2>
<ul>
<li>
Good knowledge of Apache Flink/Spark alongside good programming skills in Java/Scala
</li>
<li>
Good knowledge in a low-level programming language, e.g. C/C++
</li>
<li>
Good knowledge in various visualization techniques
</li>
</ul>

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
If everything is wired up correctly the request may take a while to finish and should eventually create messages in the 
Queue. If you exposed the port for RabbitMQ's admin page (8090:15672) you can check whether there are messages in the queue.
```
http://localhost:8090/#/queues/%2F/BDAPRO2
```
