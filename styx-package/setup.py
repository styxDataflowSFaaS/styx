import setuptools

setuptools.setup(
    name="styx",
    version="0.0.1",
    author="styx",
    packages=setuptools.find_packages(),
    install_requires=[
        'cloudpickle>=2.1.0,<3.0.0',
        'msgspec>=0.15.0,<1.0.0',
        'aiokafka>=0.8.0,<1.0',
    ],
    python_requires='>=3.10',
)
