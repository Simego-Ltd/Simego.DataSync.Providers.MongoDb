# Simego.DataSync.Providers.MongoDb
Data Sync Provider for MongoDb

This code contains two Mongo connectors available that you install together.
The first enables you to connect to a collection inside Mongo DB by providing the connection string, selecting the database and then selecting the collection to connect to.
The second, the Podio Data connector, enables you to read Podio App Items data stored within your Mongo collection.  

## Installing the Connector 
We have built a connector installer inside Data Sync that will download and install the relevant files for you.

To get to the installer go to **File** > **Install Data Connector**.
This will open the Connector Installer Window, where all you need to do is select the connector you want to install from the drop down and click **OK**.


![Install Mongo Connector](https://user-images.githubusercontent.com/63856275/226577593-9e98149f-4fac-4fdd-8d48-deb30a88338b.png) "Install Mongo Connector")


If the installation was successful you should get a confirmation popup and you now need to close all instances of Data Sync and then re-start the program. 

![Connector Installed Successfully](https://user-images.githubusercontent.com/63856275/226577891-fe8b9d1e-0275-46a9-b235-68210e17ac10.png "Connector Installed Successfully")

> If you get an error saying it could not install the connector because it is "Unable to access folder", then this is because the folder is locked by Ouvvi. 
> Please stop your Ouvvi services and try again.

You can now access both of the connectors from the connection window under **Mongo Db**.

![Mongo Connector](https://user-images.githubusercontent.com/63856275/226578349-634a61d7-cfc2-4b7a-8ae3-adc4dff2ebb2.png "Mongo Connector")

## Using the Connector

To connect to your Mongo databases you will need your connection string. You can find this in Mongo by going to **Databases** > **Connect** > **Connect your Application**.

Copy the connection string and make sure to edit it so that you change ```<password>``` to use your actual password.

![Mongo Connection String](https://user-images.githubusercontent.com/63856275/226576196-357d873c-dd06-43e3-a7a3-2946682be8d6.png "Mongo Connection String")

Enter this connection string into the **ConnectionString** field. If the connection string is valid you should now be able to view a list of your Databases in the  **Database** drop down.
Select the database to connect to and then select the **Collection** you want to connect to.

Once you are done click **Connect & Create Library Connection** to save the connection to your Mongo DB in the connection library. This means you can use it again in another project and access all of your collections from the connection library.

![Mongo DB Connector](https://user-images.githubusercontent.com/63856275/226681169-e971b1a8-d25e-4284-8940-8be80dc19bd9.png "Mongo DB Connector")

## Getting Data into a Clean Database
If you have a clean collection in your database with no data in it, you will need to ensure that one document is added to Mongo so then Data Sync can discover a schema (the columns).
One record needs to be uploaded as a JSON file into your collection.

To get a JSON file filter the source data to return one record e.g. ```ProductID == 1```.

Then go to the schema map and click onto **Preview A**, and then click onto the **Export JSON** button in the preview tab.

![Export JSON](https://user-images.githubusercontent.com/63856275/226681953-e56945e3-7934-428b-9681-397ecd650f57.png "Export JSON")

Open this file and copy the data from within the array, then go to Mongo to import it.

![JSON Data to Copy](https://user-images.githubusercontent.com/63856275/226576480-4d6bed98-92db-46eb-9d64-2ba8a08aa83a.png "JSON Data to Copy")

In Mongo, click onto your collection and then select **INSERT DOCUMENT**. 
Click onto the curly braces to switch to the JSON view, remove the text already there and paste the JSON we copied a moment ago into the window. Click **Insert** to insert it.

![Insert Document](https://user-images.githubusercontent.com/63856275/226576601-901417e3-9644-4990-8d5d-a74f61eb2d16.png "Insert Document")

Now in Data Sync, refresh your connection by clicking onto the refresh button in the data source toolbar. You should now have columns available and ready to map.

![Columns Available](https://user-images.githubusercontent.com/63856275/226682786-8f404bb1-bed1-4ad0-b784-fe93298bd2fb.png "Columns Available")

If you already have data in the collection, with all the columns you need, then you can simply connect and map the source columns to the columns in Mongo.
