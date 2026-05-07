from setuptools import find_packages, setup

package_name = 'master_capstone'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='jwg',
    maintainer_email='wjddnrud4487@kw.ac.kr',
    description='TODO: Package description',
    license='TODO: License declaration',
    extras_require={
        'test': [
            'pytest',
        ],
    },
    entry_points={
        'console_scripts': [
            'manual_master = master_capstone.manual_master:main',
            'dummy_task_node = master_capstone.dummy_task_node:main',
            'master_0 = master_capstone.master_0:main',
        ],
    },
)
