package Net::Amazon::SimpleQueue;
use strict;
use LWP::UserAgent;
use URI;
use URI::QueryParam;
use XML::LibXML;
use XML::LibXML::XPathContext;
our $VERSION = "0.29";
use base qw(Class::Accessor::Fast);
__PACKAGE__->mk_accessors(qw(libxml subscription_id ua));

sub new {
  my($class, $subscription_id) = @_;
  my $self = {};
  bless $self, $class;

  my $ua = LWP::UserAgent->new;
  $ua->timeout(30);
  $self->ua($ua);

  $self->libxml(XML::LibXML->new);

  $self->subscription_id($subscription_id);

  return $self;
}

sub create_queue {
  my($self, %options) = @_;

  my $parms = {
    Operation => 'CreateQueue',
  };
  $parms->{QueueName} = $options{name} if $options{name};
  $parms->{ReadLockTimeoutSeconds } = $options{read_lock_timeout} if $options{read_lock_timeout};

  my $xpc = $self->_request($parms);
  my $queue_id = $xpc->findvalue("//aws:CreateQueueResult/aws:QueueId");
  return $queue_id;
}

sub delete_queue {
  my($self, %options) = @_;

  my $parms = {
    Operation => 'DeleteQueue',
  };
  $parms->{QueueName} = $options{name} if $options{name};
  $parms->{QueueId}   = $options{id}   if $options{id};

  $self->_request($parms);
}

sub configure_queue {
  my($self, %options) = @_;

  my $parms = {
    Operation => 'ConfigureQueue',
  };
  $parms->{QueueName} = $options{name} if $options{name};
  $parms->{QueueId}   = $options{id}   if $options{id};
  $parms->{ReadLockTimeoutSeconds } = $options{read_lock_timeout} if $options{read_lock_timeout};

  $self->_request($parms);
}

sub list_my_queues {
  my($self, %options) = @_;

  my $parms = {
    Operation => 'ListMyQueues',
  };
  $parms->{QueueNamePrefix} = $options{prefix} if $options{prefix};

  my $xpc = $self->_request($parms);

  my @queues;

  foreach my $node ($xpc->findnodes("//aws:Queue")) {
#      warn $node->toString(1);
    my $id = $xpc->findvalue(".//aws:QueueId", $node);
    my $name = $xpc->findvalue(".//aws:QueueName", $node);
    my $read_lock_timeout = $xpc->findvalue(".//aws:ReadLockTimeoutSeconds", $node);
    push @queues, {
      id => $id,
      name => $name,
      read_lock_timeout => $read_lock_timeout,
    };
  }
  return @queues;
}

sub read {
  my($self, %options) = @_;

  my $parms = {
    Operation => 'Read',
  };
  $parms->{QueueName} = $options{name}  if $options{name};
  $parms->{QueueId}   = $options{id}    if $options{id};
  $parms->{ReadCount} = $options{count} if $options{count};

  my $xpc;
  eval { $xpc = $self->_request($parms) };
  if ($@ =~ /AWS.SimpleQueueService.NoData/) {
    return;
  } elsif ($@) {
    die $@;
  }

  my @entries;

  foreach my $node ($xpc->findnodes("//aws:QueueEntry")) {
#      warn $node->toString(1);
    my $id = $xpc->findvalue(".//aws:QueueEntryId", $node);
    my $body = $xpc->findvalue(".//aws:QueueEntryBody", $node);
    push @entries, {
      id => $id,
      body => $body,
    };
  }

  if ($options{count} && $options{count} > 1) {
    return @entries;
  } else {
    return $entries[0];
  }
}

sub enqueue {
  my($self, %options) = @_;

  my $parms = {
    Operation => 'Enqueue',
    QueueEntryBody => $options{body},
  };
  $parms->{QueueName} = $options{name} if $options{name};
  $parms->{QueueId}   = $options{id}   if $options{id};
  $self->_request($parms);
}

sub dequeue {
  my($self, %options) = @_;

  my $parms = {
    Operation => 'Dequeue',
    QueueEntryId => $options{entry_id},
  };
  $parms->{QueueName} = $options{name} if $options{name};
  $parms->{QueueId}   = $options{id}   if $options{id};
  $self->_request($parms);
}

sub _request {
  my($self, $parms) = @_;
#  sleep 1;

  $parms->{SubscriptionId} = $self->subscription_id;

  my $url = 'http://webservices.amazon.com/onca/xml?Service=AWSSimpleQueueService';

  my $uri = URI->new($url);
  $uri->query_param($_, $parms->{$_}) foreach keys %$parms;
  my $response = $self->ua->get("$uri");

#  die $uri;

  die "Error fetching response: " . $response->status_line unless $response->is_success;

  my $xml = $response->content;
  my $doc = $self->libxml->parse_string($xml);

  my $xpc = XML::LibXML::XPathContext->new($doc);
  $xpc->registerNs('aws', 'http://webservices.amazon.com/AWSSimpleQueueService/2004-10-14');

#  warn $doc->toString(1);

  if ($xpc->findnodes("//aws:SimpleQueueServiceError")) {
    die $xpc->findvalue("//aws:SimpleQueueServiceError/aws:ErrorCode") . ": " .
      $xpc->findvalue("//aws:SimpleQueueServiceError/aws:ReasonText");
  }

  return $xpc;
}


1;

__END__

=head1 NAME

Net::Amazon::SimpleQueue - Use the Amazon Simple Queue Service

=head1 SYNOPSIS

  use Net::Amazon::SimpleQueue;
  my $sq = Net::Amazon::SimpleQueue->new($subscription_id);

  my @queues = $sq->list_my_queues();

  my $queue_id = $sq->create_queue(name => $queue_name);

  $sq->configure_queue(
    name => $queue_name,
    read_lock_timeout => 65,
  );

  $sq->enqueue(
    name => $queue_name,
    body => "here is the actual data",
  );

  my $entry = $sq->read(name => $queue_name);
  $sq->dequeue(name => $queue_name, entry_id => $entry->{id});

  my @entries = $sq->read(name => $queue_name, count => 25);

  $sq->delete_queue(name => $queue_name);

=head1 DESCRIPTION

The Net::Amazon::SimpleQueue module allows you to use the Amazon
Simple Queue Service.

The Amazon Simple Queue Service provides a means for web service
applications to quickly and reliably queue resources generated by a
component to be consumed by another component. A queue can serve as a
buffer for data flowing from one component to another, even when the
producer is generating output faster than the consumer is retrieving
it. A single queue can be used simultaneously by many distributed
application components, with no need for those components to
coordinate with each other to share the queue.

In order to access the Simple Queue Service, you will need an Amazon
Web Services Subscription ID. See
http://www.amazon.com/gp/aws/landing.html

Registered developers have free access to the Simple Queue Service
during its Beta period, but storage is limited to 4,000 queue entries per
developer.

There are some limitations, so be sure to read the The Amazon Simple
Queue Service FAQ.

=head1 INTERFACE

The interface follows. Most of this documentation was copied from the
API reference. Upon errors, an exception is thrown.

=head2 new

The constructor method creates a new Net::Amazon::SimpleQueue
object. You must pass in an Amazon Web Services Subscription ID. See
http://www.amazon.com/gp/aws/landing.html:

  my $sq = Net::Amazon::SimpleQueue->new($subscription_id);

=head2 create_queue

The create_queue method creates a new queue. An optional queue name
may be provided to associate with the queue for future reference. All
queues are assigned a queue ID, which may be used to refer to the
queue:

  my $queue_id  = $sq->create_queue();
  my $queue_id2 = $sq->create_queue(name => $queue_name);

=head2 list_my_queues

The list_my_queues method returns information about queues for the
given subscription ID. If called with a queue name prefix, only
information about queues whose name begins with the prefix will be
returned. The operation returns the queue ID, the queue name (if any),
and the queue's current configuration (such as the read lock timeout),
for each queue as a list of hash references.

This operation returns a maximum of 10,000 queues. If you have more
than 10,000 queues, use the queue name prefix to narrow your search:

  my @queues = $sq->list_my_queues();
  foreach my $queue (@queues) {
    print $queue->{id} . ": " . $queue->{name} . "\n";
  }

  my @queues2 = $sq->list_my_queues(prefix = "/Ninjas/");

=head2 delete_queue

The delete_queue method deletes an empty queue from the system. If the
queue being deleted still contains data, the operation will fail. All
entries must be dequeued before the queue can be deleted. It may take
some time, upwards to 60 seconds, until you can successfully delete a
queue after removing all of the entries from a queue:

  $sq->delete_queue(id => $queue_id);

  $sq->delete_queue(name => $queue_name);

=head2 configure_queue

The configure_queue method adjusts the configuration of an existing
queue. A queue can be configured to use the specified number of
seconds as its read lock timeout:

  $sq->configure_queue(
    id => $queue_id,
    read_lock_timeout => 65,
  );

  $sq->configure_queue(
    name => $queue_name,
    read_lock_timeout => 30,
  );

=head2 enqueue

The enqueue method puts an entry into a queue. A successfully added
value is subsequently available to consumers using the read
method. Queue entries contain up to 4KB of text:

  $sq->enqueue(
    name => $queue_name,
    body => "here is the actual data",
  );

  $sq->enqueue(
    id   => $queue_id,
    body => "and some more",
  );


=head2 read

The read method returns a queue entry from a queue.

Applications should be prepared for the event where the same message
is read more than once from the queue. A message may be returned by
the read method even though it has already been dequeued, and
concurrent read calls may return the same message to multiple
readers. This behavior is a result of prioritizing reliable data
storage (even in the face of hardware failures), and we expect such
events to occur very infrequently. One way applications can cope with
these occasional duplicates is by making the messages stored in the
queue idempotent, that is, by ensuring that the effect of repeated
receipt of a message is the same as that of receiving it once.

A read lock feature is included to lower the incidence of duplicate
messages when multiple applications are concurrently reading from the
same queue. After a successful read, the queue element is locked for a
timeout period (60 seconds by default, or configurable using the
create_queue or configure_queue operations). While locked, the queue
entry will not be returned by another call to read(). This gives the
consumer application the opportunity to process the value and remove
it (with the dequeue method). If the application is unable to process
and remove the value, the read lock expires and the value is made
available on a future read attempt.

The read method tries to return queue entries that were added earlier
before returning entries added later. However, read may return entries
in a different order that they were added.

If specified, the read method will attempt to return as many entries
as is specified by the count parameter. Fewer entries may be returned
if there are fewer entries available or if not all of the entries are
available. In the later case, you may want to call the read method
again. Up to 25 entries may be read with one call to the read method.

Note that the webservice will throw an AWS.SimpleQueueService.NoData
exception if the queue is empty. In an attempt to be more Perlish,
instead of throwing this exception, the module returns undef or ().

  my $entry      = $sq->read(name => $queue_name);
  die "Nothing to read!" unless $entry;
  my $entry_id   = $entry->{id};
  my $entry_body = $entry->{body};

  my $entry2 = $sq->read(name => $queue_id);

  my @entries = $sq->read(name => $queue_name, count => 25);

=head2 dequeue

The dequeue method removes entries from a queue. An entry is
identified by its queue entry ID, which is returned with the data from
the read method.

In rare cases, the same queue entry may be returned by multiple read
calls, even for data that has already been dequeued. This behavior is
a result of prioritizing reliable data storage even in the face of
network or hardware failures. If your application sees queue data that
was previously removed, it should call the dequeue operation again.

  $sq->dequeue(name => $queue_name, entry_id => $entry->{id});

=head1 BUGS AND LIMITATIONS                                                     
                                                                                
No bugs have been reported.                                                     
                                                                                
Please report any bugs or feature requests to                                   
C<bug-<Net-Amazon-SimpleQueue>@rt.cpan.org>, or through the web interface at                   
L<http://rt.cpan.org>.  

=head1 AUTHOR

Leon Brocard C<acme@astray.com>

=head1 LICENCE AND COPYRIGHT                                                    
                                                                                
Copyright (c) 2005, Leon Brocard C<acme@astray.com>. All rights reserved.           
                                                                                
This module is free software; you can redistribute it and/or                    
modify it under the same terms as Perl itself.                                  
                                                                                
=head1 DISCLAIMER OF WARRANTY                                                   

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY          
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN        
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES          
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER               
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED                
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE  
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH           
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL            
NECESSARY SERVICING, REPAIR, OR CORRECTION.                                     
                                                                                
IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING           
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR             
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE                 
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,          
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE             
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING           
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A            
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF            
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF               
SUCH DAMAGES.
