#Web-server for SubCons

##Description:
    This is the web-server implementation of the SubCons workflow.
    The web-server is developed with Django (>=1.6.4)


    This software is open source and licensed under the GPL license

##Author
Nanjiang Shu

Short-term bioinformatics support at NBIS

Email: nanjiang.shu@scilifelab.se


## Installation

1. Install dependencies for the web server
    * Apache
    * mod\_wsgi

2. Install the virtual environments by 

    $ bash setup_virtualenv.sh

3. Create the django database db.sqlite3

4. Run 

    $ bash init.sh

    to initialize the working folder

5. In the folder `proj`, create a softlink of the setting script.

    For development version

        $ ln -s dev_settings.py settings.py

    For release version

        $ ln -s pro_settings.py settings.py

    Note: for the release version, you need to create a file with secret key
    and stored at `/etc/django_pro_secret_key.txt`

6.  On the computational node. run 


    $ virtualenv env --system-site-packages

    to make sure that python can use all other system-wide installed packages

