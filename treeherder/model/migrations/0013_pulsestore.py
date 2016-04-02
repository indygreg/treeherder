# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('model', '0012_longer_platform_names'),
    ]

    operations = [
        migrations.CreateModel(
            name='PulseStore',
            fields=[
                ('id', models.AutoField(serialize=False, primary_key=True)),
                ('revision', models.CharField(max_length=40, db_index=True)),
                ('message', models.TextField()),
                ('created', models.DateTimeField(auto_now_add=True, db_index=True)),
                ('repository', models.ForeignKey(to='model.Repository')),
            ],
            options={
                'db_table': 'pulse_store',
            },
        ),
    ]
