#!/usr/bin/env perl

BEGIN {
   die "The PERCONA_TOOLKIT_BRANCH environment variable is not set.\n"
      unless $ENV{PERCONA_TOOLKIT_BRANCH} && -d $ENV{PERCONA_TOOLKIT_BRANCH};
   unshift @INC, "$ENV{PERCONA_TOOLKIT_BRANCH}/lib";
};

binmode(STDIN, ':utf8') or die "Can't binmode(STDIN, ':utf8'): $OS_ERROR";
binmode(STDOUT, ':utf8') or die "Can't binmode(STDOUT, ':utf8'): $OS_ERROR";

use strict;
use utf8;
use Encode qw(decode encode);
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Test::More;

use PerconaTest;
use Sandbox;
require "$trunk/bin/pt-table-sync";
require VersionParser;

my $dp = new DSNParser(opts=>$dsn_opts);
my $sb = new Sandbox(basedir => '/tmp', DSNParser => $dp);
my $master_dbh = $sb->get_dbh_for('master');
my $slave1_dbh = $sb->get_dbh_for('slave1'); 
my $slave2_dbh = $sb->get_dbh_for('slave2'); 

if ( !$master_dbh ) {
   plan skip_all => 'Cannot connect to sandbox master';
}
elsif ( !$slave1_dbh ) {
   plan skip_all => 'Cannot connect to sandbox slave1';
}
elsif ( !$slave1_dbh ) {
   plan skip_all => 'Cannot connect to sandbox slave2';
}
else {
   plan tests => 5;
}


my ($output, $status);
my @args = ('--sync-to-master', 'h=127.1,P=12346,u=msandbox,p=msandbox',
            qw(-t test.t1 --print --execute --charset utf8));

# use lib/samples dir since the main change is in DSNParser
$sb->load_file('master', "t/lib/samples/charset.sql");

my $put = encode('UTF-8','абвгд');
my $want = 'абвгд';
my $row;

$master_dbh->do("SET NAMES 'utf8'");
$slave1_dbh->do("SET NAMES 'utf8'");
$slave1_dbh->do("SET NAMES 'utf8'");

$master_dbh->do("INSERT INTO test.t1 VALUES (NULL, '$put')");
$sb->wait_for_slaves();

$slave1_dbh->do("DELETE FROM test.t1 WHERE id=1 LIMIT 1");
$slave1_dbh->do("FLUSH TABLES");


# 1
($output, $status) = full_output(
   sub { pt_table_sync::main(@args) },
);

like(
   $output,
   qr/REPLACE INTO `test`.`t1`/,
   "PT-1256 Set the correct charset"
);

$sb->wait_for_slaves();

SKIP: {
   my $vp = VersionParser->new($master_dbh);
   if ($vp->cmp('8.0') > -1 && $vp->cmp('8.0.14') < 0 && $vp->flavor() !~ m/maria/i) {
      skip "Skipping in MySQL 8.0.4-rc - 8.0.13 since there is an error in the server itself", 3;
   }
   # 2
   $row = $slave1_dbh->selectrow_hashref("SELECT f2 FROM test.t1 WHERE id = 1");
   is(
      $row->{f2},
      $want,
      "Character set is correct",
   ) or diag("Want '".($want||"")."', got '".($row->{f2}||"")."'");

    # 3
    $output = `$trunk/bin/pt-table-sync --execute --lock-and-rename h=127.1,P=12345,u=msandbox,p=msandbox,D=test,t=t1 t=t2 2>&1`;
    $output = `/tmp/12345/use -e 'show create table test.t2'`;
    like($output, qr/COMMENT='test1'/, '--lock-and-rename worked');

    $sb->wait_for_slaves();
    
    #4
    $row = $slave1_dbh->selectrow_hashref("SELECT f2 FROM test.t2 WHERE id = 1");
    is(
        $row->{f2},
        $want,
        "Character set is correct",
    );
}
# #############################################################################
# Done.
# #############################################################################
$sb->wipe_clean($master_dbh);
ok($sb->ok(), "Sandbox servers") or BAIL_OUT(__FILE__ . " broke the sandbox");
exit;
