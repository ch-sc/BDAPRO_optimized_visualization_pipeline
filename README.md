<h1>Optimizing Data-Intensive Visualization Pipelines using Pixel-Perfect Compression</h1>
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


<h2>RabbitMQ</h2>
Start the docker image: (EXPOSE 5672:5672 8090:15672)

```
docker run -d --name rabbitmq --hostname my-rabbit -p 5672:5672 -p 8090:15672 -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password rabbitmq:3-management
```

Enter web UI @ [localhost:8090] and create an Exchange 'BDAPRO' and a Queue 'BDAPRO'. Then create a binding for both.



<h2>References</h2>
[1] U. Jugel et. al.; M4: A Visualization-Oriented Time Series Data Aggregation;
URL: http://www.vldb.org/pvldb/vol7/p797-jugel.pdf

[2] J. Traub et. al.; I2: Interactive Real-Time Visualization for Streaming Data; 
URL: https://openproceedings.org/2017/conf/edbt/paper-276.pdf

[3] Eugene Wu:
Data Visualization Management Systems. CIDR 2015

[4] Eugene Wu, Fotis Psallidas, Zhengjie Miao, Haoci Zhang, Laura Rettig:
Combining Design and Performance in a Data Visualization Management System. CIDR 2017

[5] Arvind Satyanarayan, Dominik Moritz, Kanit Wongsuphasawat, Jeffrey Heer:
Vega-Lite: A Grammar of Interactive Graphics. IEEE Trans. Vis. Comput. Graph. 23(1): 341-350 (2017)

[6] Vega-Lite: A Grammar of Interactive Graphics: https://www.domoritz.de/talks/VegaLite-OpenVisConf-2017.pdf

[7] Eleni Tzirita Zacharatou, Harish Doraiswamy, Anastasia Ailamaki, Cláudio T. Silva, Juliana Freire:
GPU Rasterization for Real-Time Spatial Aggregation over Arbitrary Polygons. 352-365

[8]  G. Burtini, S. Fazackerley, and R. Lawrence. Time series compression for adaptive chart generation. In CCECE, pages 1–6. IEEE, 2013.
