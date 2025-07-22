# openshift-ansible-operator-plugins is built from the openshift/Dockerfile
# FROM registry.redhat.io/openshift4/ose-ansible-rhel9-operator@sha256:8be7197200e8275f280a90a3bcf39bd1d9d6356055b325f8428e3bbb72cc72ed
FROM quay.io/ckyal/ansible-operator-plugins:jul20-c

COPY testdata/memcached-molecule-operator/requirements.yml ${HOME}/requirements.yml
RUN ansible-galaxy collection install -r ${HOME}/requirements.yml \
    && chmod -R ug+rwx ${HOME}/.ansible

COPY testdata/memcached-molecule-operator/watches.yaml ${HOME}/watches.yaml
COPY testdata/memcached-molecule-operator/roles/ ${HOME}/roles/
COPY testdata/memcached-molecule-operator/playbooks/ ${HOME}/playbooks/
