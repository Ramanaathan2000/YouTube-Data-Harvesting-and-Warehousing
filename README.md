# YouTube-Data-Harvesting-and-Warehousing

# Overview
This project is designed to harvest and warehouse YouTube data using Python programming language, PostgreSQL, MongoDB, and Streamlit. The goal is to collect, store, and analyze YouTube data for various purposes such as content recommendation, trend analysis, and user behavior insights.

# Tools Used
# Python Programming Language: 
Python is used for its versatility, ease of use, and a rich ecosystem of libraries, making it suitable for web scraping, data processing, and analysis.

# Google API Client:
The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

# PostgreSQL: 
PostgreSQL is employed as the relational database management system (RDBMS) to store structured data efficiently. It provides ACID compliance and supports complex queries.

# MongoDB:
MongoDB is used as the NoSQL database to store semi-structured and unstructured data. Its flexible schema and scalability make it suitable for handling diverse types of data.

# Streamlit:
Streamlit is utilized for building interactive and customizable web-based data dashboards. It allows for easy visualization of the harvested YouTube data.

# Youtube Data Scrapping and its Ethical Perspective: 
When engaging in the scraping of YouTube content, it is crucial to approach it ethically and responsibly. Respecting YouTube's terms and conditions, obtaining appropriate authorization, and adhering to data protection regulations are fundamental considerations. The collected data must be handled responsibly, ensuring privacy, confidentiality, and preventing any form of misuse or misrepresentation. Furthermore, it is important to take into account the potential impact on the platform and its community, striving for a fair and sustainable scraping process. By following these ethical guidelines, we can uphold integrity while extracting valuable insights from YouTube data.

# Required Libraries
-> googleapiclient.discovery
-> streamlit
-> psycopg2
-> pymongo
-> pandas

# Packages required for Project to be installed:
-> Google Api Client :pip3 install google-client-api or python3 -m pip install google-client-api
-> Pandas    :   pip install pandas
-> MongoDB    :  pip install pymongo
-> PostgreSql :  pip install psycopg2
-> Streamlit  :  pip install streamlit


# Features
# 1.YouTube Data Harvesting:
# Video Metadata:
Retrieve information such as video title, description, publish date, view count, and likes/dislikes.
# Channel Information:
Collect details about the channel, including name, description, subscriber count, and upload frequency.
# Comments and Engagement:
Harvest comments, replies, and engagement metrics for each video.
# Thumbnails and Images:
Download thumbnails and additional images associated with videos and channels.

# 2.Data Warehousing:
# Relational Database (PostgreSQL):
Design a schema to store video and channel information in a structured manner.
Implement data normalization to reduce redundancy and ensure data consistency.
Create tables for video metadata, channel details, comments, and engagement metrics.

# NoSQL Database (MongoDB):
Store semi-structured data like video comments and replies in MongoDB's flexible document format.
Utilize MongoDB's indexing and querying capabilities for efficient retrieval.

# 3.Streamlit Dashboard:

# Interactive Visualizations:
Create dynamic charts, graphs, and tables to visualize key metrics such as views, likes, and comments over time.
# User Interaction:
Allow users to filter and sort data based on various parameters.
# Real-Time Updates:
Implement real-time data updates as new YouTube data is harvested.

# 4.User Authentication:
Implement user authentication for the Streamlit dashboard to control access and protect sensitive information.

# 5.Automation:
-> Schedule periodic data harvesting using tools like cron jobs or task schedulers to keep the dataset up-to-date.
-> Implement error handling and logging to capture issues during the data harvesting process.

# 6.API Integration:
Integrate with YouTube API for more efficient and authorized data retrieval.

# 7.Scalability:
-> Design the system to handle a growing dataset efficiently.
-> Consider using cloud-based solutions for databases to facilitate scalability.

# 8.Documentation:
-> Provide detailed documentation on how to set up, configure, and use the project.
-> Include explanations of the data schema, data flow, and any external APIs used.

# 9.Security:
-> Ensure secure handling of API keys and credentials.
-> Implement encryption for sensitive data.

# Project Output :
https://youtube/2uzlKhkd-B0

# Linkedin:
https://www.linkedin.com/in/ramanaathan-s-61425117b?lipi=urn%3Ali%3Apage%3Ad_flagship3_profile_view_base_contact_details%3BrZrjWUVOS9SfAN70YzHLvA%3D%3D







