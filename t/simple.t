#!perl
use strict;
use warnings;
use IO::Prompt;
use Test::More;
use Test::Exception;

my $subscription_id;

eval {
  local $SIG{ALRM} = sub { die "alarm\n" };
  alarm 60;
  $subscription_id = prompt("Please enter an AWS subscription ID for testing: ");
  alarm 0;
};

if ($subscription_id && length($subscription_id) == 20) {
  eval 'use Test::More tests => 28;';
} else {
  eval 'use Test::More plan skip_all => "Need AWS subscription ID for testing, skipping"';
}

use_ok("Net::Amazon::SimpleQueue");

my $queue_name = "Net::Amazon::SimpleQueue test suite";

my $sq = Net::Amazon::SimpleQueue->new($subscription_id);
isa_ok($sq, "Net::Amazon::SimpleQueue", "Have an object back");

my $queue_id = $sq->create_queue();
ok($queue_id, "Created an unamed queue with id $queue_id");
$sq->delete_queue(id => $queue_id);
ok(1, "Deleted queue $queue_id");

eval { $sq->delete_queue(name => $queue_name) };
if ($@ =~ /AWS.SimpleQueueService.NonEmptyQueue/) {
  my @entries = $sq->read(name => $queue_name, count => 25);
  $sq->dequeue(name => $queue_name, entry_id => $_->{id}) foreach @entries;
  while (1) {
    diag("Trying to delete queue named $queue_name (might take a few tries)");
    eval { $sq->delete_queue(name => $queue_name) };
    warn $@ if $@;
    last unless $@;
    sleep 10;
  }
}
ok(1, "Preemtively deleted queue with name $queue_name");

$queue_id = $sq->create_queue(name => $queue_name);
ok($queue_id, "Created a $queue_name queue with id $queue_id");
$sq->delete_queue(name => $queue_name);
ok(1, "Deleted queue $queue_name");

$queue_id = $sq->create_queue(name => $queue_name);
ok($queue_id, "Created a $queue_name queue with id $queue_id");

throws_ok { $sq->create_queue(name => $queue_name ) } qr/AWS.SimpleQueueService.QueueNameExists/,
  "Threw exception upon trying to create duplicate queue named $queue_name";

$sq->configure_queue(
  name => $queue_name,
  read_lock_timeout => 61,
);
ok(1, "Configured read_lock_timeout on queue named $queue_name");

$sq->configure_queue(
  id => $queue_id,
  read_lock_timeout => 5,
);
ok(1, "Configured read_lock_timeout on queue with id $queue_id");

my @queues = $sq->list_my_queues();
#use YAML; die Dump(\@queues);
ok(scalar(@queues), "Returned list of my queues");
ok((grep { $_->{name} eq $queue_name } @queues), "List contains queue named $queue_name");
ok((grep { $_->{id} eq $queue_id } @queues), "List contains queue with id $queue_id");

@queues = $sq->list_my_queues(prefix => "/No/Ninjas/Here/");
ok(!scalar(@queues), "Returned no queues starting with /No/Ninjas/Here/");

my $entry = $sq->read(name => $queue_name);
ok(!$entry, "Reading returned nothing on queue named $queue_name");

$entry = $sq->read(id => $queue_id);
ok(!$entry, "Reading returned nothing on queue with id $queue_id");

my $rand = rand(1000);
$sq->enqueue(
  name => $queue_name,
  body => "$rand testing <b>one</b>",
);
ok(1, "Enqueued data to queue named $queue_name");
$sq->enqueue(
  id   => $queue_id,
  body => "$rand testing <b>two</b>",
);
ok(1, "Enqueued data to queue with id $queue_id");

sleep 1;

$entry = $sq->read(name => $queue_name);
ok($entry, "Reading returned entry on queue named $queue_name");
like($entry->{body}, qr{$rand testing <b>(one|two)</b>}, "Entry is one of our test entries");
$sq->dequeue(name => $queue_name, entry_id => $entry->{id});
ok(1, "Dequeued " . $entry->{id} . " on queue named $queue_name");

$entry = $sq->read(id => $queue_id);
ok($entry, "Reading returned entry on queue with id $queue_id");
like($entry->{body}, qr{$rand testing <b>(one|two)</b>}, "Entry is one of our test entries");
$sq->dequeue(id => $queue_id, entry_id => $entry->{id});
ok(1, "Dequeued " . $entry->{id} . " on queue with id $queue_id");

$entry = $sq->read(name => $queue_name);
ok(!$entry, "Reading returned nothing on queue named $queue_name");

$entry = $sq->read(id => $queue_id);
ok(!$entry, "Reading returned nothing on queue with id $queue_id");

while (1) {
  diag("Trying to delete queue named $queue_name (might take a few tries)");
  eval { $sq->delete_queue(name => $queue_name) };
  last unless $@;
  sleep 10;
}
ok(1, "Delete queue named $queue_name\n");
