from setuptools import find_packages, setup

from bioos.__about__ import __version__

setup(
    name="pybioos",
    version=__version__,
    keywords=["pip", "bioos"],
    description="BioOS SDK for Python",
    license="MIT Licence",
    url="https://github.com/GBA-BI/pybioos",
    author="Jilong Liu",
    author_email="liu_jilong@gzlab.ac.cn",
    packages=find_packages(),
    platforms="any",
    install_requires=[
        "volcengine>=1.0.61", "tabulate>=0.8.10", "click>=8.0.0",
        "pandas>=1.3.0", "tos==2.5.6", "cachetools>=5.2.0",
        "typing-extensions>=4.4.0", "apscheduler>=3.10.4", "colorama>=0.4.6"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Topic :: Scientific/Engineering :: Interface Engine/Protocol Translator"
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'bw=bioos.bioos_workflow:bioos_workflow',
            'bw_import=bioos.bw_import:bioos_workflow_import',
            'bw_import_status_check=bioos.bw_import_status_check:bioos_workflow_status_check',
            'bw_status_check=bioos.bw_status_check:bioos_workflow_status_check',
            'get_submission_logs=bioos.get_submission_logs:get_submission_logs',
            'list_bioos_workspaces=bioos.cli.list_bioos_workspaces:main',
            'list_workflows_from_workspace=bioos.cli.list_workflows_from_workspace:main',
            'list_submissions_from_workspace=bioos.cli.list_submissions_from_workspace:main',
            'generate_inputs_json_template_bioos=bioos.cli.generate_inputs_json_template_bioos:main',
            'create_workspace_bioos=bioos.cli.create_workspace_bioos:main',
            'list_files_from_workspace=bioos.cli.list_files_from_workspace:main',
            'download_files_from_workspace=bioos.cli.download_files_from_workspace:main',
            'create_iesapp=bioos.cli.create_iesapp:main',
            'check_ies_status=bioos.cli.check_ies_status:main',
            'get_ies_events=bioos.cli.get_ies_events:main',
            'get_workspace_profile=bioos.cli.get_workspace_profile:main',
            'validate_wdl=bioos.cli.validate_wdl:main',
            'delete_submission=bioos.cli.delete_submission:main',
            'export_bioos_workspace=bioos.cli.export_bioos_workspace:main',
            'upload_dashboard_file=bioos.cli.upload_dashboard_file:main',
            'search_dockstore=bioos.cli.search_dockstore:main',
            'fetch_wdl_from_dockstore=bioos.cli.fetch_wdl_from_dockstore:main',
            'get_docker_image_url=bioos.cli.get_docker_image_url:main',
            'build_docker_image=bioos.cli.build_docker_image:main',
            'check_build_status=bioos.cli.check_build_status:main'
        ]
    })
