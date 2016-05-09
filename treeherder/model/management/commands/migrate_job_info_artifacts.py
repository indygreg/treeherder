import json
import time
from optparse import make_option

from django.core.management.base import BaseCommand

from treeherder.model.derived import ArtifactsModel
from treeherder.model.models import (Datasource,
                                     Job,
                                     JobDetail,
                                     Repository)


class Command(BaseCommand):

    help = 'Migrate existing jobs to intermediate jobs table'
    option_list = BaseCommand.option_list + (
        make_option('--project',
                    action='append',
                    dest='project',
                    help='Filter deletion to particular project(s)',
                    type='string'),
        make_option('--interval',
                    dest='interval',
                    help='Wait specified number of seconds between job info migrations',
                    type='float',
                    default=0.0))

    def handle(self, *args, **options):
        if options['project']:
            datasources = Datasource.objects.filter(
                project__in=options['project'])
        else:
            datasources = Datasource.objects.all()
        for ds in datasources:
            self.stdout.write('{}\n'.format(ds.project))
            repository = Repository.objects.get(name=ds.project)
            offset = 0
            limit = 10000
            while True:
                job_id_pairs = Job.objects.order_by(
                    'project_specific_id').filter(
                        project_specific_id__gt=offset,
                        repository=repository).values_list(
                            'id', 'project_specific_id')[:limit]
                if len(job_id_pairs) == 0:
                    break
                job_ids = set([job_id_pair[1] for job_id_pair in job_id_pairs])
                max_job_id = max(job_ids)
                # filter out those job ids for which we already have
                # generated job details
                job_ids -= set(JobDetail.objects.filter(
                    job__repository=repository,
                    job__project_specific_id__in=job_ids).values_list(
                        'job__project_specific_id', flat=True))
                if job_ids:
                    job_id_mapping = dict((project_specific_id, job_id) for
                                          (job_id, project_specific_id) in
                                          job_id_pairs)
                    with ArtifactsModel(ds.project) as am:
                        am.DEBUG = False
                        artifacts = am.get_job_artifact_list(0, 10000, {
                            'job_id': set([('IN', tuple(job_ids))]),
                            'name': set([("=", "Job Info")])})
                        job_details = []
                        for artifact in artifacts:
                            for job_detail_dict in artifact['blob']['job_details']:
                                metadata = {
                                    'title': job_detail_dict.get('title'),
                                    'value': job_detail_dict['value'],
                                    'url': job_detail_dict.get('url')
                                }
                                max_field_length = JobDetail.MAX_FIELD_LENGTH
                                for (name, val) in metadata.iteritems():
                                    if type(val) != str:
                                        metadata[name] = val = json.dumps(val)
                                    if val is not None and len(val) > max_field_length:
                                        print "WARNING: element for {} '{}' too long, truncating".format(
                                            artifact['job_id'], val)
                                        metadata[name] = val[:max_field_length]
                                job_details.append(JobDetail(
                                    job_id=job_id_mapping[artifact['job_id']],
                                    title=metadata['title'],
                                    value=metadata['value'],
                                    url=metadata['url']))
                        JobDetail.objects.bulk_create(job_details)
                self.stdout.write('{} '.format(offset))
                self.stdout.flush()
                offset = max_job_id
            self.stdout.write("\n")
        time.sleep(options['interval'])
