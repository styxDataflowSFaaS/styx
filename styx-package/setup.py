import setuptools

setuptools.setup(
    name="styx",
    version="0.0.1",
    author="",
    packages=setuptools.find_packages(),
    install_requires=[
        'cloudpickle>=2.1.0>,<3.0.0',
        'msgpack>=1.0.3,<2.0.0',
        'aiokafka>=0.8.0,<1.0',
    ],
    python_requires='>=3.8',
)
