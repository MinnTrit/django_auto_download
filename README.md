# Sample video
https://github.com/MinnTrit/django_auto_download/assets/151976884/fb7c9f48-c626-47f1-8917-a97800033a76

# Overall diagram
![image](https://github.com/MinnTrit/django_auto_download/assets/151976884/4b88255c-927e-457c-9043-9424211db712)

# Diagram's breakdown
```Notes```: This is the updated version, all the back-end jobs are compressed to the Huey worker, all of which will be executed by this worker when it recieves the POST request
### Workflow:
1. Requests made by the users: When the users make the request to the webserver, it will be forward to the ```urls.py``` of Django framework
2. Mapping endpoint with the views function:
   * ```Views functions``` are Django's back-end functions, normally used to perform certain users-defined logic to response to the request coming from the users
   * Regarding the certain structure, the ```Views functions``` are mainly used to retrieve the parameters sent by the users, which in this case are the ```start_date```, ```end_date```, ```seller_id```, and finally the ```launcher```
3. View function perform according actions:
   * After receving the users' parameters, the view function will forward these paramaters to the ```Huey Task Queue Manager```
     * The ```Huey Task Queue Manager``` is built to run asynchronously, meaning that this process will happen besides the main thread, allowing the views function to continue with its logic
     * Thanks to this feature, the view function is able to ```redirect``` the users back to the home page, otherwise, the page will continue loading until the task executed by the worker is done
   * After passing these params to the ```Huey Task Queue Manager```, the views function can continue redirecting the users to the home page asynchronously
4. ```Huey Task Queue Manager``` puts the parameters with the assigned task to ```Redis Message Broker```:
   * Normally, ```Redis``` is used as the cache database for the back-end, but in this case, it's used as the ```Message Broker```, mostly used to store the queued tasks
   * Thanks to ```Redis```, this will allow upcoming tasks not going to be missed, but rather stored on ```Redis``` and wait for idle workers (if any) to pick them up and execute the task
5. ```Huey Worker``` listens on ```Redis``` to pick up any upcoming tasks to execute:
   * Since we have the ```Huey Worker``` connected to the ```Redis``` server through the set up, it has been running in the back-end background
   * when the task arrived, it will be informed and pick up the task from ```Redis``` to execute
