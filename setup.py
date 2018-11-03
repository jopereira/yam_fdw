
from setuptools import setup

if __name__ == '__main__':
    setup(name='xyam_fdw',
          author='Jose Orlando Pereira',
          author_email='jop@di.uminho.pt',
          description='FDW for MongoDB',
          version='0.1.0',
          install_requires=['pymongo>=2.8.1',
                            'python-dateutil'],
          packages=['xyam_fdw'])

