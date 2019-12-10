from django.db import models
from django import forms

from django.core.validators import MinLengthValidator, MaxLengthValidator 

# Create your models here.

class Query(models.Model):# {{{
    raw_seq = models.CharField(max_length=20000)
    seqname = models.CharField(max_length=100)
    submit_date = models.DateTimeField('date submitted')
# }}}
class SubmissionForm(forms.Form):# {{{
    """
    Defining the form to submit queries
    """
    rawseq = forms.CharField(label='', max_length=100000,
            widget=forms.Textarea(attrs={'cols': 62, 'rows': 10}),
            required=False)
    seqfile = forms.FileField(label="Alternatively, upload a text file in FASTA format upto 10 MB", required=False)
    jobname = forms.CharField(label='Job name (optional)', max_length=100, required=False)
    email = forms.EmailField(label='Email (recommended for batch submissions)', max_length=100, required=False)
# }}}
class SubmissionForm_findjob(forms.Form):# {{{
    """
    Defining the form for findjob
    """
    jobid = forms.CharField(
            label='Job ID',
            widget=forms.TextInput(attrs={'placeholder': 'Input job ID (rst_xxxxx)'}), 
            max_length=12, 
            required=False)
    jobname = forms.CharField(
            label='Job name', 
            widget=forms.TextInput(attrs={'placeholder': 'Input job name'}), 
            max_length=30, 
            required=False)
# }}}
class FieldContainer(models.Model):# {{{
# This class is modified from the Spyne example written by BJ Cardon
# Copyright BJ Cardon <bj dot car dot don at gmail dot com>,
# All rights reserved.
    char_field = models.CharField(max_length=32, default='test')
    char_field_nullable = models.CharField(max_length=32, null=True)
    slug_field = models.SlugField(max_length=32, unique=True)
    text_field = models.TextField(default='text_field')
    email_field = models.EmailField()
    boolean_field = models.BooleanField(default=True)
    integer_field = models.IntegerField(default=1)
    positive_integer_field = models.PositiveIntegerField(default=1)
    float_field = models.FloatField(default=1)
    decimal_field = models.DecimalField(max_digits=10, decimal_places=4,
            default=1)
    time_field = models.TimeField(auto_now_add=True)
    date_field = models.DateField(auto_now_add=True)
    datetime_field = models.DateTimeField(auto_now_add=True)

    foreign_key = models.ForeignKey('self', null=True,
            related_name='related_containers', on_delete=models.CASCADE)
    one_to_one_field = models.OneToOneField('self', null=True,
            on_delete=models.CASCADE)

    custom_foreign_key = models.ForeignKey('RelatedFieldContainer', null=True,
            related_name='related_fieldcontainers', on_delete=models.CASCADE)
    custom_one_to_one_field = models.OneToOneField('RelatedFieldContainer',
            null=True, on_delete=models.CASCADE)

    url_field = models.URLField(default='http://example.com')
    file_field = models.FileField(upload_to='test_file', null=True)
    excluded_field = models.CharField(max_length=32, default='excluded')
    blank_field = models.CharField(max_length=32, blank=True)
    length_validators_field = models.CharField(
            max_length=32, null=True, validators=[MinLengthValidator(3),
                MaxLengthValidator(10)])
# }}}
class RelatedFieldContainer(models.Model):# {{{
# This class is modified from the Spyne example written by BJ Cardon
# Copyright BJ Cardon <bj dot car dot don at gmail dot com>,
# All rights reserved.
    id = models.CharField(max_length=30, primary_key=True)

# }}}
