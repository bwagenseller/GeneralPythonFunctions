# General USe

These are general Python functions I use. 

# Assumptions

> I am assuming you are using a derivative of Linux for your machine that is running Python; if yo uare running Windows or a Mac, you will have to do some things differently (like adding the PYTHONPATH variable or installing Anaconda / other packages).

These files assume that you have the [PYTHONPATH](https://bwagenseller.github.io/#/ubuntu/server_build?id=adding-to-python-path) set in your [.profile](https://bwagenseller.github.io/#/ubuntu/linux_notes?id=variables-in-profile) (or similar environment file) and also assumes you have placed these files in the folder that is specified in your PYTHONPATH.

Finally, they generally assume you have installed basic SciPy packages (most notably Pandas and Numpy); a great way to do this is to [install Anaconda](https://bwagenseller.github.io/#/ubuntu/server_build?id=python-anaconda-install).

## Database Assumptions

I am assuming that you have the [mysql.connector package](https://bwagenseller.github.io/#/ubuntu/server_build?id=install-mysql-for-python) installed for MySQL; in addition, if you wish to use the Oracle connections you must install the Oracle instant client as well as the Python [cx_Oracle package](https://bwagenseller.github.io/#/ubuntu/server_build?id=install-oracle-for-python).
