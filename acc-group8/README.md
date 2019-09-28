# QTL-as-a-Service project - Group 8
Contributors: Elsa Bergman, Anton Carlsson, Alexander Groth, Swathi Kamani 

This repository contains code for the QTL-as-a-Service application. It consists of a Heat template (Heat_template_start_instance.yml) and a python script (qtlaas_api.py). It also contains a script which installs everything needed on the user's VM, from which the QTLaaS application is accessible. 

The python script qtlaas_api.py consists of two API calls, one to deploy a stack and one for deleting the stack. The application uses Flask and to deploy a stack, you need to run the server and use the curl command and the correct API (/qtlaas/start/<string:stack\_name> or /qtlaas/stop) to start the process of deploying or deleting a stack.
