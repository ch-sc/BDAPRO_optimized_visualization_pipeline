<h1>Optimizing Data-Intensive Visualization Pipelines using Pixel-Perfect Compression</h1>
<h2>GROUP 11</h2>

| Member            | Matriculum No.| GitHub                                 |
| :---------------- |--------------:| :--------------------------------------|
| Christoph Schulze | 389866        | [@ch-sc](https://github.com/ch-sc)     |
| Laura Mons        | 364309        | [@eleicha](https://github.com/eleicha) |

    Supervisor:
     Sebastian Breß
     @sebastianbress (https://github.com/sebastianbress)
     

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

<h2>
Deliverables
</h2>


[Report](./FinalReports)

[Midterm presentation](./Presentations/Midterm)

[Final presentation](./Presentations/Final)

[Sources](./Sources/service)

[README](./Sources/service/README.md)

[JavaDoc](./Sources/JavaDoc)

[Notebook](./Sources/notebook)

