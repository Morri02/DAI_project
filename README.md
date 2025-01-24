# DAI Project

This project implements a distributed mutual exclusion algorithm using MQTT and Lamport's protocol.

## Requirements

- Python 3.x
- `paho-mqtt` library
- MQTT broker (e.g., Mosquitto)

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/your-username/DAI_project.git
    cd DAI_project
    ```

2. Install the dependencies:

    ```sh
    pip install paho-mqtt
    ```

3. Configure the MQTT broker:

    - Ensure you have an MQTT broker running (e.g., Mosquitto).
    - Edit the [config.json](http://_vscodecontentref_/1) file with your MQTT broker settings.

4. Run the simulation:
    ```sh
    python main.py [MQTT|SOCKET]
    ```

## Configuration

The [`config.json`](config.json) file contains the necessary configurations for the project:

