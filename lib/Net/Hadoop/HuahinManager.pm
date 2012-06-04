package Net::Hadoop::HuahinManager;

use strict;
use warnings;
use Carp;

use URI::Escape qw//;
use JSON::XS qw//;

use Furl;

our $VERSION = "0.1";

sub new {
    my ($this, %opts) = @_;
    croak "Huahin Manager server name missing" unless $opts{server};

    my $self = +{
        server => $opts{server},
        port => $opts{port} || 9010,
        useragent => $opts{useragent} || "Furl Net::Hadoop::HuahinManager $VERSION",
        timeout => $opts{timeout} || 10,
    };
    $self->{furl} = Furl::HTTP->new(agent => $self->{useragent}, timeout => $self->{timeout});
    return bless $self, $this;
}

sub list {
    my ($self, $op) = @_;
    $op ||= 'all';
    my $path = '/job/list'; # for all
    if ($op eq 'failed') {
        $path = '/job/list/failed';
    } elsif ($op eq 'killed') {
        $path = '/job/list/killed';
    } elsif ($op eq 'prep') {
        $path = '/job/list/prep';
    } elsif ($op eq 'running') {
        $path = '/job/list/running';
    } elsif ($op eq 'succeeded') {
        $path = '/job/list/succeeded';
    }
    return $self->request('GET', $path);
}

sub status {
    my ($self, $jobid) = @_;
    return $self->request('GET', '/job/status/' . URI::Escape::uri_escape($jobid));
}

sub detail {
    my ($self, $jobid) = @_;
    return $self->request('GET', '/job/detail/' . URI::Escape::uri_escape($jobid));
}

sub kill {
    my ($self, $jobid) = @_;
    return $self->request('DELETE', '/job/kill/id/' . URI::Escape::uri_escape($jobid));
}

sub kill_by_name {
    my ($self, $jobname) = @_;
    return $self->request('DELETE', '/job/kill/name/' . URI::Escape::uri_escape($jobname));
}

sub request {
    my ($self, $method, $path) = @_;
    my @request_params = (
        method => $method,
        host => $self->{server},
        port => $self->{port},
        path_query => $path,
    );
    my ($ver, $code, $msg, $headers, $body) = $self->{furl}->request(@request_params);
    my $content_type = undef;
    for (my $i = 0; $i < scalar(@$headers); $i += 2) {
        if ($headers->[$i] =~ m!\Acontent-type\Z!i) {
            $content_type = $headers->[$i+1];
        }
    }

    if ($code == 200) {
        if ($content_type =~ m!^application/json! and length($body) > 0) {
            return JSON::XS::decode_json($body);
        }
        return 1;
    }
    # error
    carp "Huahin Manager returns error: $code";
    return undef;
}

1;
