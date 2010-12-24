$| = 1;
use strict;
use warnings;

use Getopt::Long;
use IO::Socket;
use EV;

use RedisSharding;

$SIG{PIPE} = "IGNORE";

my $VERBOSE = 0;


GetOptions(
	"host=s"  => \ my $local_host,
	"port=i"  => \ (my $local_port = 6379),
	"nodes=s" => \ my $nodes,
);


unless ($nodes) {
	print <<EOD;
Parameter 'nodes' is required.

Using example:
perl $0                             --nodes=10.1.1.2:6380,10.1.1.3:6380,...
perl $0                 --port=6379 --nodes=10.1.1.2:6380,10.1.1.3:6380,...
perl $0 --host=10.1.1.1 --port=6379 --nodes=10.1.1.2:6380,10.1.1.3:6380,...
EOD
	exit;
}

my @servers = split /\s*,\s*/, $nodes;

my $blksize = 1024 * 16;

my %fh2rw = ();
my %fh2ww = ();



my %c2s = ();
my %s2c = ();
my %s2a = ();


my %c2buf = ();
my %s2buf = ();

my %c2client_reader = ();
my %c2servers_reader = ();


sub clean_from_client {
	my ($c) = @_;
	print "clean_from_client\n" if $VERBOSE;
	foreach my $s (values %{$c2s{$c}}) {
		delete $fh2rw{$s};
		delete $fh2ww{$s};
		delete $s2buf{$s};
		delete $s2c{$s};
		delete $s2a{$s};
		close $s;
	}
	delete $fh2rw{$c};
	delete $fh2ww{$c};
	delete $c2buf{$c};
	delete $c2client_reader{$c};
	delete $c2servers_reader{$c};
	delete $c2s{$c};
	close $c;
}



sub write2client {
	my ($c, $buf) = @_;
	$c2buf{$c} .= $buf;
	$fh2ww{$c} ||= EV::io($c, EV::WRITE, \&c_w_event_cb);
}

sub write2server {
	my ($c, $s_addr, $buf) = @_;
	foreach my $s ($s_addr ? $c2s{$c}{$s_addr} : values %{$c2s{$c}}) {
		$s2buf{$s} .= $buf;
		$fh2ww{$s} ||= EV::io($s, EV::WRITE, \&s_w_event_cb);
	}
}


my $listener = IO::Socket::INET->new(
	Proto => 'tcp', Blocking => 0,
	LocalHost => $local_host, LocalPort => $local_port,
	Listen => 20, ReuseAddr => 1
) or die $!;



$fh2rw{$listener} = EV::io($listener, EV::READ, sub {
	my $w = shift;
	my $listener = $w->fh;

			my $c_sock = $listener->accept;
			$c_sock->sockopt(SO_KEEPALIVE, 1);
			$fh2rw{$c_sock} = EV::io($c_sock, EV::READ, \&c_r_event_cb);
			foreach (@servers) {
				my ($host, $port) = split /:/;
				my $s_sock = IO::Socket::INET->new(Proto => 'tcp', Blocking => 0, PeerHost => $host, PeerPort => $port);
				unless ($s_sock) {
					warn $!;
					clean_from_client($c_sock);
					return;
				}
				$s_sock->sockopt(SO_KEEPALIVE, 1);
				$c2s{$c_sock}{$_} = $s_sock;
				$s2c{$s_sock} = $c_sock;
				$s2a{$s_sock} = $_;
				$fh2rw{$s_sock} = EV::io($s_sock, EV::READ, \&s_r_event_cb);
			}
			($c2client_reader{$c_sock}, $c2servers_reader{$c_sock}) = readers($c_sock, \@servers, \&write2server, \&write2client, $VERBOSE);
});


sub c_r_event_cb {
	my $w = shift;
	my $fh = $w->fh;
			my $len = sysread $fh, (my $buf), $blksize;
			if ($len) {
				my $rv = $c2client_reader{$fh}->($buf);
				unless ($rv) {
					warn "ERROR: unified protocol error";
					write2client($fh, "-ERR unified protocol error\r\n");
				}
			} elsif (defined $len) {
				clean_from_client($fh);
			}
}


sub s_r_event_cb {
	my $w = shift;
	my $fh = $w->fh;
			my $c = $s2c{$fh};
			my $len = sysread $fh, (my $buf), $blksize;
			if ($len) {
				$c2servers_reader{$c}->($s2a{$fh}, $buf);
			} elsif (defined $len) {
				clean_from_client($c);
			}
}


sub s_w_event_cb {
	my $w = shift;
	my $fh = $w->fh;
			my $buf = $s2buf{$fh};
			my $len = syswrite $fh, $buf, $blksize;
			if ($len) {
				substr $buf, 0, $len, "";
				$s2buf{$fh} = $buf;
				unless (length $buf) {
					delete $fh2ww{$fh};
				}
			}		
}



sub c_w_event_cb {
	my $w = shift;
	my $fh = $w->fh;
			my $buf = $c2buf{$fh};
			my $len = syswrite $fh, $buf, $blksize;
			if ($len) {
				substr $buf, 0, $len, "";
				$c2buf{$fh} = $buf;
				unless (length $buf) {
					delete $fh2ww{$fh};
				}
			}		
}


EV::loop;
