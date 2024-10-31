import json
import http.client
import geopandas as gpd
from shapely.geometry import Point, LineString
import os
import time
import socket

API_KEY = 'rt2JR8Rds03Ry03hQTpD9j0N01gWEULJnuY3l1_GeXA8uqUVLtXsKHUQuW5ra0lt-FklrA40qq6_J04yY0nPjlfKG1uPerclUX2gf6axkIioJadYxzOG3cPZJLRcZ2_vHPdipZWvQdICAL2zRnqnOUCGjfq4Q8aMdmA7H6z7xK7W9MEKnIiEALokmtChLtr-s6hDFko17M7xihPpNlfGN7N8D___wn55epkLMtS2eFF3JPlj_SjpFIGXYK15PJFta-BmPqCFvEwXlZEYfEf8uYOpAvCEdBn3NOMoB-P28owOJ7ZeBQf5VMFi3J5_RV2fE_XDR2LTD469Qq0y3946LQ'

def getJobList():
    """Retrieve job list from KatapultPro API and include all jobs regardless of status."""
    URL_PATH = '/api/v2/jobs'
    headers = {}
    all_jobs = []

    for attempt in range(5):
        conn = None  # Initialize connection variable
        try:
            conn = http.client.HTTPSConnection("katapultpro.com", timeout=10)
            conn.request("GET", f"{URL_PATH}?api_key={API_KEY}", headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            jobs_dict = json.loads(data)

            if not isinstance(jobs_dict, dict):
                raise TypeError(f"Expected a dictionary but received {type(jobs_dict)}: {jobs_dict}")

            all_jobs = [{'id': job_id, 'name': job_details.get('name'), 'status': job_details.get('status')}
                        for job_id, job_details in jobs_dict.items()]
            break  # Exit loop if successful

        except (socket.error, OSError) as e:
            print(f"Socket error: {e}. Retrying...")
            time.sleep(5)  # Wait before retrying
        except Exception as e:
            print(f"Failed to retrieve job list: {e}")
            break  # Exit on general exception
        finally:
            if conn:
                conn.close()  # Ensure the connection is closed

    return all_jobs


def getJobData(job_id):
    """Retrieve job data for the given job ID."""
    URL_PATH = f'/api/v2/jobs/{job_id}'
    headers = {}

    for attempt in range(5):
        conn = None  # Initialize connection variable
        try:
            conn = http.client.HTTPSConnection("katapultpro.com", timeout=10)
            conn.request("GET", f"{URL_PATH}?api_key={API_KEY}", headers=headers)
            res = conn.getresponse()
            data = res.read().decode("utf-8")
            job_data = json.loads(data)

            if "error" in job_data and job_data["error"] == "RATE LIMIT EXCEEDED":
                print("Rate limit exceeded. Retrying after delay...")
                time.sleep(5)
                continue
            return job_data  # Successfully retrieved data

        except json.JSONDecodeError:
            print(f"Failed to decode JSON for job {job_id}")
        except (socket.error, OSError) as e:
            print(f"Socket error while retrieving job data for {job_id}: {e}. Retrying...")
            time.sleep(5)  # Delay before retry
        except Exception as e:
            print(f"Error retrieving job data for {job_id}: {e}")
            break
        finally:
            if conn:
                conn.close()  # Ensure the connection is closed after each attempt
            time.sleep(0.5)  # Short delay between attempts

    print(f"Failed to retrieve job data for job ID {job_id} after multiple attempts.")
    return None


def extractPoles(job_data, job_name, job_id):
    """Extract points from job data, including 'Job_Status'."""
    print(f"Extracting poles from job: {job_name}, Job ID: {job_id}")
    if isinstance(job_data, tuple):
        job_data = job_data[1]  # Unpack if job_data is a tuple
    # Get Job_Status from job data
    job_status = job_data.get('metadata', {}).get('Job_Status', "Unknown")
    nodes = job_data.get("nodes", {})

    if not nodes:
        print("No nodes available or nodes is not a list.")
        return []

    pole_points = []
    for node_id, node_data in nodes.items():
        if node_data.get('attributes', {}).get('node_type', {}).get('button_added') == 'pole':
            latitude = node_data.get('latitude')
            longitude = node_data.get('longitude')
            int_note = node_data.get('attributes', {}).get('internal_note', {}).get('button_added')
            scid = node_data.get('attributes', {}).get('scid', {}).get('auto_button')

            # Extract the 'tagtext' from the 'pole_tag' dictionary
            pole_tag = None
            pole_tag_data = node_data.get('attributes', {}).get('pole_tag', {})
            if isinstance(pole_tag_data, dict):
                # Get the first dictionary value and access 'tagtext' if it exists
                first_tag = next(iter(pole_tag_data.values()), {})
                pole_tag = first_tag.get('tagtext')

            # Append attributes, Job_Status, and job_id for mapping Job_Name
            pole_points.append({
                "Longitude": longitude,
                "Latitude": latitude,
                "MRNote": int_note,
                "PoleTag": pole_tag,
                "SCID": scid,
                "Job_Status": job_status,
                "job_id": job_id  # Include job_id for mapping Job_Name later
            })
    return pole_points


def extractAnchors(job_data, job_name, job_id):
    """Extract anchor points from job data, focusing only on the Anchor_Spec attribute."""
    anchors = job_data.get("nodes", {})
    anchor_points = []

    for node_id, node_data in anchors.items():
        # Check if node_type is 'new anchor'
        if node_data.get("attributes", {}).get("node_type", {}).get("button_added") == "new anchor":
            latitude = node_data.get("latitude")
            longitude = node_data.get("longitude")

            # Extract anchor_spec
            anchor_spec_data = node_data.get("attributes", {}).get("anchor_spec", {})
            anchor_spec = anchor_spec_data.get("button_added", "Unknown")

            # Append anchor information with Anchor_Spec and Job_Name
            anchor_points.append({
                "Longitude": longitude,
                "Latitude": latitude,
                "Anchor_Spec": anchor_spec,  # Add Anchor_Spec to capture the anchor specification
                "job_id": job_id  # Include job_id for mapping Job_Name later
            })
    return anchor_points


def savePointsToShapefile(points, filename, job_dict):
    """Save points to a shapefile in the specified workspace using geopandas, with WGS 1984 CRS."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    print(f"Attempting to save shapefile to: {file_path}")

    # Create geometries from points with latitude and longitude
    geometries = [Point(point["Longitude"], point["Latitude"]) for point in points]

    # Map Job_Name and clean PoleTag
    for point in points:
        job_id = point.get("job_id")  # Retrieve job_id from point data
        point["Job_Name"] = job_dict.get(job_id, "Unknown")  # Set Job_Name or default to "Unknown"
        point["Job_Status"] = point.get("Job_Status", "Unknown")  # Include Job_Status for each point

        if isinstance(point.get("PoleTag"), dict) and "tagtext" in point["PoleTag"]:
            point["PoleTag"] = point["PoleTag"]["tagtext"]
        else:
            point["PoleTag"] = None

        # Debug output to verify Job_Name and Job_Status mappings
        print(f"Point Data - Job_Name: {point['Job_Name']}, Job_Status: {point['Job_Status']}")

    # Create GeoDataFrame with specified columns and geometries, and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(points, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving shapefile: {e}")

def saveMasterShapefile(all_points, filename):
    """Save the combined list of points to a master shapefile, excluding the job_id field."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    # Remove the job_id field from each point
    for point in all_points:
        point.pop("job_id", None)  # Remove 'job_id' if it exists

    # Create geometries from points
    geometries = [Point(point["Longitude"], point["Latitude"]) for point in all_points]

    # Create GeoDataFrame and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(all_points, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Master shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving master shapefile: {e}")

def saveMasterAnchorShapefile(anchor_points, filename):
    """Save the combined list of anchor points to a master anchor shapefile, excluding unnecessary fields."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    # Remove the job_id field from each point
    for point in anchor_points:
        point.pop("job_id", None)  # Remove 'job_id' if it exists

        # Rename fields for Shapefile limitations
        if "Anchor_Spec" in point:
            point["Anch_Spec"] = point.pop("Anchor_Spec")  # Shorten to 10 characters

    # Create geometries from points
    geometries = [Point(point["Longitude"], point["Latitude"]) for point in anchor_points]

    # Create GeoDataFrame and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(anchor_points, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Master anchor shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving master anchor shapefile: {e}")


def extractConnections(job_data, job_name, job_id):
    """Extract line connections from job data using node references for start and end points, focusing on specific cable types."""
    connections = job_data.get("connections", {})
    nodes = job_data.get("nodes", {})
    cables = job_data.get("cables", {})

    print(f"Connections data type: {type(connections)}")

    if not isinstance(connections, dict) or not nodes:
        print(f"Unexpected structure in 'connections' or no nodes found.")
        return []

    line_connections = []

    for conn_id, connection in connections.items():
        # Skip connections with 'reference' type
        connection_type = connection.get("attributes", {}).get("connection_type", {}).get("button_added")
        if connection_type == "reference":
            print(f"Skipping connection {conn_id} with type 'reference'")
            continue  # Skip this connection

        # Fetch the node references for start and end
        start_node_id = connection.get("node_id_1")
        end_node_id = connection.get("node_id_2")

        start_node = nodes.get(start_node_id)
        end_node = nodes.get(end_node_id)

        if not start_node or not end_node:
            print(f"Missing node data for connection: start_node_id={start_node_id}, end_node_id={end_node_id}")
            continue

        start_coords = (start_node.get("longitude"), start_node.get("latitude"))
        end_coords = (end_node.get("longitude"), end_node.get("latitude"))

        if not all(start_coords) or not all(end_coords):
            print(f"Invalid coordinates: start={start_coords}, end={end_coords}")
            continue

        # Process cables to find specific wire_spec
        wire_spec = None
        cable_id = None
        for cable_id, cable_data in cables.items():
            # Match _trace to connection ID and fetch wire_spec
            if cable_data.get("_trace") == conn_id:
                wire_spec = cable_data.get("wire_spec", "No Spec Found")
                print(f"Found wire_spec '{wire_spec}' for cable ID {cable_id} in connection {conn_id}")
                break

        line_connections.append({
            "StartX": start_coords[0],
            "StartY": start_coords[1],
            "EndX": end_coords[0],
            "EndY": end_coords[1],
            "ConnType": connection_type,
            "Wire_Spec": wire_spec,
            "JobName": job_name
        })

    print(f"Total connections found: {len(line_connections)}")
    return line_connections


def saveLineShapefile(line_connections, filename, job_dict):
    """Save line connections to a shapefile with specified fields and geometry."""
    workspace_path = r"C:\Users\lewis\Documents\Deeply_Digital\Katapult_Automation\workspace"
    file_path = os.path.join(workspace_path, filename)

    print(f"Attempting to save line connections shapefile to: {file_path}")

    # Create line geometries
    geometries = [
        LineString([(line["StartX"], line["StartY"]), (line["EndX"], line["EndY"])])
        for line in line_connections
    ]

    # Prepare data for GeoDataFrame with specified fields
    line_data = [
        {
            "ConnType": line["ConnType"],
            "JobName": line["JobName"],
            "Wire_Spec": line["Wire_Spec"]
        }
        for line in line_connections
    ]

    # Create GeoDataFrame with specified columns and geometries, and set CRS to WGS 1984 (EPSG:4326)
    gdf = gpd.GeoDataFrame(line_data, geometry=geometries, crs="EPSG:4326")

    try:
        gdf.to_file(file_path, driver="ESRI Shapefile")
        print(f"Line shapefile successfully saved to: {file_path}")
    except Exception as e:
        print(f"Error saving line shapefile: {e}")


def main():
    TEST_FIRST_JOB_ONLY = True
    all_jobs = getJobList()
    all_pole_points = []  # List to store all pole points for the master shapefile
    all_anchor_points = []  # List to store all anchor points for the master shapefile
    all_line_connections = []  # List to store all line connections for the master shapefile

    # Create a job dictionary for mapping Job_Name by job_id
    job_dict = {job['id']: job['name'] for job in all_jobs}

    # Process each job
    for i, job in enumerate(all_jobs):
        job_id = job['id']
        job_name = job['name']

        print(f"Processing job: {job_name} (ID: {job_id})")

        # Fetch job data
        job_data = getJobData(job_id)
        if i == 0:
            print(json.dumps(job_data))  # Print JSON for the first job for debugging

        # If job_data retrieval was unsuccessful, skip to the next job
        if job_data is None:
            continue

        # Extract poles, anchors, and connections
        pole_points = extractPoles(job_data, job_name, job_id)
        anchor_points = extractAnchors(job_data, job_name, job_id)
        line_connections = extractConnections(job_data, job_name, job_id)

        # Map Job_Name for each point before adding to master lists
        for point in pole_points:
            point["Job_Name"] = job_dict.get(job_id, "Unknown")
        all_pole_points.extend(pole_points)  # Add pole points to the master list

        for point in anchor_points:
            point["Job_Name"] = job_dict.get(job_id, "Unknown")
        all_anchor_points.extend(anchor_points)  # Add anchor points to the master list

        # Add connections with job mapping
        for line in line_connections:
            line["Job_Name"] = job_dict.get(job_id, "Unknown")
        all_line_connections.extend(line_connections)  # Add line connections to the master list

        if TEST_FIRST_JOB_ONLY:
            break

        # Short delay between each job to reduce socket load
        time.sleep(1)

    # Save the combined points and lines to master shapefiles
    if all_pole_points:
        saveMasterShapefile(all_pole_points, 'master_poles.shp')
    else:
        print("No pole points found across jobs to save to master shapefile.")

    if all_anchor_points:
        saveMasterAnchorShapefile(all_anchor_points, 'master_anchors.shp')
    else:
        print("No anchor points found across jobs to save to master shapefile.")

    if all_line_connections:
        saveLineShapefile(all_line_connections, 'master_lines.shp', job_dict)
    else:
        print("No line connections found across jobs to save to master shapefile.")



if __name__ == '__main__':
    start_time = time.time()  # Record the start time
    main()
    end_time = time.time()  # Record the end time

    elapsed_time = end_time - start_time
    print(f"Total execution time: {elapsed_time:.2f} seconds")
