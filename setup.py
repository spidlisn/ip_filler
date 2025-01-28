from setuptools import setup, find_packages

setup(
    name="ip_db_filler",
    version="1.0.0",
    py_modules=['ip_db_filler'],
    install_requires=[
        'aiomysql==0.2.0',
        'boto3==1.36.7',
        'botocore==1.36.7',
        'cryptography==44.0.0',
        'greenlet==3.1.1',
        'SQLAlchemy==2.0.37',
        'tqdm==4.67.1'
    ],
    python_requires='>=3.8',
    entry_points={
        'console_scripts': [
            'ip_db_filler=ip_db_filler:main',
        ],
    }
)
