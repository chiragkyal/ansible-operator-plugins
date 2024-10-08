FROM registry.ci.openshift.org/ocp/builder:rhel-8-golang-1.22-openshift-4.17 AS builder

ENV GO111MODULE=on \
    GOFLAGS=-mod=vendor

COPY . /go/src/github.com/openshift/ansible-operator-plugins
RUN cd /go/src/github.com/openshift/ansible-operator-plugins \
 && make build

FROM registry.ci.openshift.org/ocp/4.17:base

RUN mkdir -p /etc/ansible \
    && echo "localhost ansible_connection=local" > /etc/ansible/hosts \
    && echo '[defaults]' > /etc/ansible/ansible.cfg \
    && echo 'roles_path = /opt/ansible/roles' >> /etc/ansible/ansible.cfg \
    && echo 'library = /usr/share/ansible/openshift' >> /etc/ansible/ansible.cfg

ENV HOME=/opt/ansible \
    USER_NAME=ansible \
    USER_UID=1001

RUN yum install -y \
      ansible \
      ansible-runner \
      ansible-runner-http \
      python3-kubernetes \
      python3-openshift \
      python3-oauthlib \
      python3-requests-oauthlib \
      python3-jmespath \
      tini \
 && yum clean all \
 && rm -rf /var/cache/yum

COPY --from=builder /go/src/github.com/openshift/ansible-operator-plugins/ansible-operator /usr/local/bin/ansible-operator
COPY openshift/release/ansible/ansible_collections ${HOME}/.ansible/collections/ansible_collections

COPY openshift/release/ansible/bin /usr/local/bin

RUN chmod +x /usr/local/bin/user_setup && \
    ./usr/local/bin/user_setup

# Ensure directory permissions are properly set
RUN mkdir -p ${HOME}/.ansible/tmp \
 && chown -R ${USER_UID}:0 ${HOME} \
 && chmod -R ug+rwx ${HOME}

WORKDIR ${HOME}
USER ${USER_UID}
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/ansible-operator", "run", "--watches-file=./watches.yaml"]
