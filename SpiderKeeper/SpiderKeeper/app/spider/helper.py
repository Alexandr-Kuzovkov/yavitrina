from SpiderKeeper.app import scheduler, app, agent, db
from SpiderKeeper.app.spider.model import Project, JobInstance, SpiderInstance, JobExecution, Option



options_types = {
    'dont_run_duplicate': 'BOOLEAN',
    'hidden_spiders': 'LIST',
    'date_count': 'INTEGER'
}

# check duplicate
def is_spider_duplicate(job_instance):
    dont_run_duplicate = Option.get_option_value('dont_run_duplicate', 'BOOLEAN')
    if dont_run_duplicate is not None and dont_run_duplicate:
        runned_jobs = JobExecution.list_uncomplete_job()
        if runned_jobs is not None and len(runned_jobs) > 0:
            for runned_job in runned_jobs:
                curr_job_instance = JobInstance.query.filter_by(project_id=job_instance.project_id, id=runned_job.job_instance_id).first()
                if curr_job_instance is not None:
                    if curr_job_instance.spider_name == job_instance.spider_name and curr_job_instance.spider_arguments == job_instance.spider_arguments:
                        return True
    return False