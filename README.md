<h1>Stock vs Oil Price Correlation Analysis</h1>
<p>
  This project combines ETL processes with analytical workflows to uncover the relationship 
  between stock price changes and oil price changes. The ETL pipeline is orchestrated using 
  <strong>Airflow</strong>, deployed with <strong>Docker</strong> and <strong>Azure Container Instance</strong>, and leveraging <strong>Azure PostgreSQL</strong> 
  as the database backend.
</p>

<h2>✨ Key Features</h2>
    <ul>
        <li><strong>ETL with Airflow</strong>: Fetch and transform data using Airflow DAGs.</li>
        <li><strong>Containerized Deployment</strong>: Easily deploy the workflow using Docker and Azure Container Instance.</li>
        <li><strong>Data Normalization and Analysis</strong>: Perform Z-Score normalization and correlation analysis of stock and oil price changes.</li>
        <li><strong>Azure Integration</strong>: Store and manage data in Azure PostgreSQL for scalability and reliability.</li>
        <li><strong>Data Visualization</strong>: Plot raw and normalized trends, and display scatter plots for relationships.</li>
    </ul>

<h2>🛠 Requirements</h2>
    <ul>
        <li>Python 3.8+</li>
        <li>Docker</li>
        <li>Apache Airflow</li>
        <li>Azure PostgreSQL Database</li>
        <li>Python Libraries: <code>psycopg2-binary</code>, <code>pandas</code>, <code>matplotlib</code>, <code>beautifulsoup4</code>, <code>requests</code>, <code>pymysql</code></li>
    </ul>
    <pre><code>pip install psycopg2-binary pandas matplotlib beautifulsoup4 requests pymysql</code></pre>

<h2>🚀 How to Run</h2>
    <ol>
        <li>
            <strong>Set Up the Database:</strong>  
            Ensure your Azure PostgreSQL database contains the required tables:
            <ul>
                <li><code>company_stocks</code>: Contains columns <code>stock_change</code>, <code>datetime</code>, <code>ticker</code>, <code>open</code>, <code>price</code>, <code>average_stock_price</code>.</li>
                <li><code>oil_prices</code>: Contains columns <code>price_change</code>, <code>datetime</code>, <code>price_id</code>, <code>type</code>, <code>price_change</code>, <code>normalized_price</code>.</li>
            </ul>
        </li>
        <li>
          <strong>Set Up the Docker and Airflow:</strong>
          Ensure your docker machine is running and run the following command:
          <ul>
            <li><code>docker pull puckel/docker-airflow</code></li>
            <li><code>docker run -d -p 8080:8080 puckel/docker-airflow webserver</code> (you should be able to see Airflow run on localhost:8080)</li>
            <li>Download mains_dag.py from this repository (make sure to change database credential with your own)</li>
            <li><code>docker cp path_to_mains_dag.py container_name:/usr/local/airflow/dags</code></li>
            <li><code>docker exec -it container_name bash (go to inside container)</code></li>
            <li><code>pip install psycopg2-binary pandas matplotlib beautifulsoup4 requests pymysql</code></li>
            <li><code>docker restart container_name</code></li>
            <li>Go to localhost:8080 and turn on the dag</li>
          </ul>
        </li>
        <li>
          <strong>Check data on your database</strong>
        </li>
        <li>
          <strong>Get the visualization:</strong>
          <ul>
            <li>Download correlation_analysis.py from this repository</li>
            <li>Run the code from correlation_analysis.py (make sure to change database credential with your own)</li>
          </ul>
        </li>
    </ol>

<h2>🔗 Resources</h2>
    <ul>
        <li><a href="https://airflow.apache.org/">Apache Airflow Documentation</a></li>
        <li><a href="https://www.docker.com/">Docker Documentation</a></li>
        <li><a href="https://learn.microsoft.com/en-us/azure/postgresql/">Azure PostgreSQL Documentation</a></li>
    </ul>

<h2>📜 Note</h2>
  <ul>
    <li>Link to Notion: <a href="https://www.notion.so/Data-Pipelining-1442890c34e88091bf36f315f63a5946?pvs=4">Click_me</a></li>
    <li>Example output: <a href="https://drive.google.com/file/d/1XB0T8u_LjSNzabtKT83_xW2H886irf26/view?usp=sharing">Click_me</a></li>
  </ul>
