package DBA::Backup::mysql;

use 5.006001;
use strict;
use warnings;
use Compress::Zlib; # replace shelling to gzip?
use DBA::Backup;

our $VERSION = '0.2_1';

=head1

NOTICE! This is currently a broken partial port from the origal working
MySQL specific module. I hope to have the port finished and a functional
version uploaded soon. Email me or the list for more information.

The mailing list for the DBA modules is perl-dba@fini.net. See
http://lists.fini.net/mailman/listinfo/perl-dba to subscribe.

=begin dev

Required methods (can be void if appropriate):
* <server>_flush_logs # so all logs are current
<server>_rotate_logs # rotate specified log types
<server>_dump_databases # as server optimized SQL file (locking option)
<server>_stop_server
<server>_start_server
<server>_lock_database


=end dev

=cut

=head1 mysql_flush_logs

Uses mysqladmin refresh to flush all logs and tables.

=cut
sub mysql_flush_logs {
	my $self = shift;
	my $HR_conf = shift;
	
	# prepare the flush command
	my $cmd_path = $HR_conf->{mysqladmin}{path};
	my $username = $HR_conf->{connect}{USER};
	my $password = $HR_conf->{connect}{PASSWORD};
	my $db_host = $HR_conf->{connect}{RDBMS_HOST};
	my $socket = $HR_conf->{connect}{SOCKET};
	my $cmd = "$cmd_path -u$username -p$password --host=$db_host "
		. "--socket=$socket "; 
	$cmd .= join(' ', map { "--$_" }
		@{$HR_conf->{mysqladmin}{options}});
	$cmd .= " refresh";
	
	$self->_debug("flushing logs with $cmd");
	# execute the flush command
	if (-x $cmd_path) {
		my $rc = system($cmd);
		$cmd =~ s/$password/xxxxxx/;
		$self->_error("$rc recieved on $cmd") if $rc;
		print $self->{LOG} "Completed: $cmd";
	} # if we're allowed to execute
	else {
		$self->_error("mysqladmin is not executable.");
	} # else bitch
	
	return 0;
} # mysql_flush_logs

=head1 mysql_rotate_logs

Rotates the binary update, error and any extra mysql logs specified in the
conf file. Rotation of binary and error logs is not optional on runs when the
databases get backed up. Error and binary logs are kept as incrementals.
Other logs are just appended, and are cleared and restarted once over a certain
size (as defined in conf).

=cut
sub mysql_rotate_logs {
	my $self = shift;
	my $HR_conf = shift;
	
	# check the list of currently running mysql queries
	print $self->{LOG} "\n\n*Current processlist*\n";
	$self->_debug("_get_process_list",2);
	print $self->{LOG} $self->_get_process_list($HR_conf);
	
	my $base_log_dir = $HR_conf->{backup_params}{LOG_DIR};
	my $hostname = $HR_conf->{connect}{RDBMS_HOST} eq 'localhost'
		? $self->{HOSTNAME} : $HR_conf->{connect}{RDBMS_HOST};
	
	print $self->{LOG} "\n\n*Rotating logs*\n";
	$self->_debug("_rotate_general_query_log?");
	$self->_rotate_log("General query",
			"$base_log_dir/$hostname .log",
			$HR_conf->{backup_params}{MAX_GEN_LOG_SIZE},
			$HR_conf->{backup_params}{MAX_GEN_LOG_FILES})
		if $HR_conf->{backup_params}{ROTATE_GEN_QUERY_LOGS} =~ /yes/i;
	
	$self->_debug("_rotate_slow_query_log?");
	$self->_rotate_slow_query_log()
		if $HR_conf->{backup_params}{ROTATE_SLOW_QUERY_LOGS} =~ /yes/i;
	
	$self->_debug("_cycle_bin_logs?");
	my $cur_day = substr(localtime,0,3);
	if (($self->{backup_params}{CYCLE_BIN_LOGS_DAILY} =~ /^yes$/i ) 
			or (grep(/$cur_day/i, @{$HR_conf->{days}}))) {
		$self->_cycle_bin_logs();
	} # if bin logs backed up daily or today is a full dump day
	
	
	$self->_debug("_rotate_error_log?");
	$self->_rotate_error_log()
		if $HR_conf->{backup_params}{ROTATE_ERROR_LOGS} =~ /yes/i;
	
	return 0;
} # mysql_rotate_logs


###
### Internal functions
###


## _get_process_list() ##
#
# Returns a list of all mysql processes running currently on the server.
#
# Gets the processlist from dbms and print it to the LOG the fields are
# as follows:
# Id  User Host db Command Time State Info
#
# The assumption is that these fields will not change. It's hard to make
# a dynamic script because LISTFIELDS works only on tables, and retrieval
# to a hash does not preserve the order of the fields.
#
sub _get_process_list {
	my $self = shift;
	my $HR_conf = shift;
	
	# prepare the flush command
	my $cmd_path = $HR_conf->{mysqladmin}{path};
	my $username = $HR_conf->{connect}{USER};
	my $password = $HR_conf->{connect}{PASSWORD};
	my $db_host = $HR_conf->{connect}{RDBMS_HOST};
	my $socket = $HR_conf->{connect}{SOCKET};
	my $cmd = "$cmd_path -u$username -p$password --host=$db_host "
		. "--socket=$socket "; 
	$cmd .= join(' ', map { "--$_" }
		@{$HR_conf->{mysqladmin}{options}});
	$cmd .= " flush-logs flush-hosts";
	
	$self->_debug("Getting process list with $cmd");
	# execute the flush command
	my $mesg = '';
	if (-x $cmd_path) {
		$mesg = `$cmd`;
		$cmd =~ s/$password/xxxxxx/;
		$self->_error("$rc recieved on $cmd") if $?;
		$self->_debug("Completed: $cmd");
	} # if we're allowed to execute
	else {
		$self->_error("mysqladmin is not executable.");
	} # else bitch
	
	return $mesg;
} # end _get_process_list()





###
### unprocessed original subs
###


sub _rotate_log {
	my $self = shift;
	
	my $logname = shift;
	my $log_file = shift;
	my $max_log_size = shift;
	my $max_log_count = shift;

	# test if file exists
	unless (-f $log_file) {
		print $self->{LOG} "$logname log doesn't exist\n";
		return;
	} # only rotate if it exists
	print $self->{LOG} "$logname log is $log_file\n";

	# test if file is larger than max log size
	unless (-s $log_file > ($max_log_size*1024*1024) ) {
		print $self->{LOG} "$logname log did not need rotating\n";
		return;
	} # rotate log if larger than max log size

	# rename all of the old logs, keep only as many of them as set by the
	# config parameter
	for (my $i = $max_log_count - 2; $i >= 0; $i--) {
		my $j = $i + 1; 
		if (-f "$log_file.$i") {
			print $self->{LOG} "Renaming $log_file.$i to $log_file.$j\n"; 
			rename("$log_file.$i", "$log_file.$j");
		} # if log at iteration exists, move down
	} # for each possible log iteration
	
	# rename the current log to .0
	rename($log_file, "$log_file.0");
	
	# done
	print $self->{LOG} "$logname log rotated\n";

} # end _rotate_generic_log()

sub _rotate_slow_query_log {
	my $self = shift;
	
	$self->_rotate_generic_log("Slow query",
			$self->{backup_params}{ROTATE_SLOW_QUERY_LOGS},
			$self->{backup_params}{LOG_DIR} . '/'
			. $self->{db_connect}{HOSTNAME} . '.log',
			$self->{backup_params}{MAX_SLOW_LOG_SIZE},
			$self->{backup_params}{MAX_SLOW_LOG_FILES});
} # end _rotate_slow_query_log()

=head1 _rotate_error_log()

	The mysql error logs don't operate the same way as the other logs.
	As of mysql 4.0.10, every flush-logs command will cause the error log
	to rotate to a file with an "-old" suffix attached.  This is
	regardless of the file's size.  Mysql shutdown/startup will *not*
	rotate the error log to the -old file.  Any previous -old file
	is deleted.

	This function attempts to restore some sanity to how mysql treats
	the error log.  Call this function after the flush-logs command.
	We will take new -old file and append it to the end of our own file,
	(different name) and delete the -old file.  We'll then call the usual
	_rotate_generic_log function on it.

=cut 

sub _rotate_error_log {
	my $self = shift;
	
	my $log_dir = $self->{backup_params}{LOG_DIR};
	my $hostname = $self->{db_connect}{HOSTNAME};
	my $log_in  = $log_dir . '/' . $hostname . '.err-old';
	my $log_out = $log_dir . '/' . $hostname . '-error.log';
	print $self->{LOG} "\n";
	
	# test if file exists
	unless (-f $log_in) {
		print $self->{LOG} "mysqld old error log ($log_in) doesn't exist\n";
		return;
	} # if old err log doesn't exist
	print $self->{LOG} "mysqld old error log is $log_in\n";
	print $self->{LOG} "... merging into cumulative error log $log_out\n";
	
	# merge mysql droppings into our own log file
	open(INFILE,$log_in) or $self->_error("Problem reading $log_in: $!");
	open(OUTFILE,">>$log_out") or $self->_error("Problem appending $log_out: $!");
	while (<INFILE>) { print OUTFILE $_; }
	close OUTFILE or $self->_gripe("$!");
	close INFILE or $self->_gripe("$!");
	unlink($log_in);
	
	# perform usual log rotation on merged file
	_rotate_generic_log("Cumulative error",
			$self->{backup_params}{ROTATE_ERROR_LOGS},
			$log_dir . '/' . $hostname . '.log',
			$self->{backup_params}{MAX_ERROR_LOG_SIZE},
			$self->{backup_params}{MAX_ERROR_LOG_FILES});
} # end _rotate_error_logs()


=head1 _cycle_bin_logs()

	Issues command to mysqld to finish writing to the current binary
	update log and start writing to a new one.  We then push all of
	the bin-logs (except for the newest one) into [dump_dir]/00/.

	The flush logs command causes mysqld to close the old (already renamed)
	general query and slow query logs and reopen the logs of the usual
	file name.  It also causes mysqld to flush the binary update log and
	begin writing to a new binlog file.  It does not affect the error 
	log, only a restart of mysqld will start a new error log.

	The flush hosts command will clean up the hosts cache.

=cut 

sub _cycle_bin_logs {
	my $self = shift;
	
	my ($hostname) = $self->{db_connect}{HOSTNAME} =~ m/^([^\.]+)/;
	my $data_dir = $self->{backup_params}{LOG_DIR};	
	my $dump_dir = $self->{backup_params}{DUMP_DIR} . '/00';
	
	# get a list of all existing binlog files to back up
	opendir(DIR, $data_dir) 
		or $self->_error("Cannot open directory where bin log files reside\n");
	my @binlog_files = grep { /$hostname\-bin\.\d+/ } readdir(DIR);
	closedir(DIR);
	$self->_debug("Found @binlog_files in $data_dir");
	
	# back up the binary update logs
	$self->_debug('backing up bin log');
	print $self->{LOG} "\nBacking up binary update logs\n";
	print $self->{LOG} "Moving binlogs from $data_dir/ to $dump_dir/ ...\n";
	foreach my $file (@binlog_files) {
		my $rc = File::Copy::move("$data_dir/$file", "$dump_dir/$file");
		if ($rc) {
			print $self->{LOG} "... moved $file\n";	
		} # if move succesful
		else {
			$self->_error("Can't move the binary log file $file - $!($rc)\n");
		} # else die
	} # foreach bin log
	print $self->{LOG} "Backed up " . int(@binlog_files) . " binary update logs\n";
	
} # end _cycle_bin_logs()


=head1 _backup_databases()

	Backup all databases on the server DBMS which are mentioned 
	explicitly or as a pattern in the [included-databases] section 
	in the config file.

	This function will dump all specified databases to .sql.gz files
	in the directory [dump_dir]/new/.  If there were no errors during
	backup, _rotate_dump_dirs will then rename it [dump_dir]/00/.

	If this function encounters errors during backup, the partial dumps 
	to [dump_dir]/new/ will remain until the next time this function is
	executed.  At that time, the contents of [dump_dir]/new/ will be
	destroyed and new dumps will be placed there.

	At no time are binary update logs ever placed in [dump_dir]/new/.

	Return with the number of errors encountered during backup.

=cut

sub _backup_databases {
	my $self = shift;
	
	my $dump_dir = $self->{backup_params}{DUMP_DIR} . '/new';
	my $backup_errors = 0;
	
	# create the new/ dump_dir, but delete it if it already exists
	if (-d $dump_dir) {
		print $self->{LOG} "Partial/failed dumps in $dump_dir exist, deleting...\n";
		eval { File::Path::rmtree($dump_dir) };
		$self->_error("Cannot delete $dump_dir - $@\n") if ($@);
		$self->_error("$dump_dir deleted, but still exists!\n") if (-d $dump_dir);
	} # if directory exists
	$self->_debug('_test_create_dirs');
	$self->_test_create_dirs($dump_dir);
	
	# dump a .sql.gz file for each database into the dump_dir
	foreach my $database ( @{$self->{backup}{databases}} ) {
		$self->_debug("Backing up $database");
		
		# get the date, parsed into its parts
		my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) 
			= localtime();
		$year += 1900; $mon += 1;
		
		# build part of the output file name from the date parts
		my $date_spec = $year . sprintf("%02d%02d%02d%02d", $mon, $mday, 
			$hour, $min);
		my $time_stamp = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
			$year,$mon,$mday,$hour,$min,$sec);
		my $db_host = $self->{db_connect}{RDBMS_HOST};
		my $hostname = $self->{db_connect}{HOSTNAME};
		(my $short_hostname) = $hostname =~ s/^([^\.]+).*$/$1/;
		my $dump_file = $date_spec .'_'. $short_hostname .'_'. $database 
			. '.sql.gz';
		
		print $self->{LOG} "[$time_stamp] Dumping $database to $dump_file\n";
		
		# build the dump command line in steps
		my $gzip = $self->{bin_dir}{gzip};
		my $mysqldump = $self->{bin_dir}{mysqldump};
		my $username = $self->{db_connect}{USER};
		my $password = $self->{db_connect}{PASSWORD};
		my $socket = $self->{db_connect}{SOCKET};
		
		my $cmd = "$mysqldump -u$username -p$password --host=$db_host "
			. "--socket=$socket"; 
		$cmd .= join(' ',
			map { "--$_" } @{$self->{mysqldump}{options}});
		$cmd .= "$database | $gzip -9 > $dump_dir/$dump_file";
		
		# make sure that the database backup went fine
		$self->_debug("Dumping with $cmd");
		my $rc = system($cmd);
		if ($rc) {
			$cmd =~ s/ -p$password / -pPASSWORD_HIDDEN /;
			print $self->{LOG} 'An error occured while backing up database '
				. "$database - $rc - '$cmd'\n";
			$backup_errors++;
		} # if there was an error executing command
		
	} # foreach $database
	
	# print timestamp one more time when it's all done
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
	$year += 1900; $mon += 1;
	my $time_stamp = sprintf("%04d-%02d-%02d %02d:%02d:%02d", 
				 $year,$mon,$mday,$hour,$min,$sec);
	print $self->{LOG} "[$time_stamp] Compressed dumps to $dump_dir/ "
	. "completed with $backup_errors errors\n";
	
	return $backup_errors;
} # end _backup_databases()



           binmode STDOUT;     # gzopen only sets it on the fd

           my $gz = gzopen(\*STDOUT, "wb")
                 or die "Cannot open stdout: $gzerrno\n" ;

           while (<>) {
               $gz->gzwrite($_)
               or die "error writing: $gzerrno\n" ;
           }

           $gz->gzclose ;
		#my $zipped = Compress::Zlib::memGzip($dumped_db);

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

DBA::Backup::mysql - MySQL server extension for DBA::Backup

=head1 SYNOPSIS

This module is not used directly. It is a server extension for the DBA::Backup
module. See the instalation directions for more information.

=head1 DESCRIPTION


=head1 HISTORY

=over 8

=item 0.01

Original version; created by h2xs 1.23 with options

  -ACXn
	DBA::Backup::mysql

=item 0.1

Partially ported from original functional MySQL specific module. Currently
broken, but I wanted to get the structure set up and uploaded to CPAN.

=item 0.1.1

Some more work on getting port working and some comments. Early release
for boston.pm mailing list.

=item 0.1.2

Improved configuration handling allowing multiple mysql servers. Sample
configuration file updated to reflect change. Methods now expect server
specific configuration to be passed explicitly and no longer look up
in $self->{mysql_server} directly.

=back



=head1 SEE ALSO

The mailing list for the DBA modules is perl-dba@fini.net. See
http://lists.fini.net/mailman/listinfo/perl-dba to subscribe.

=head1 AUTHOR

Sean Quinlan, E<lt>gilant@gmail.comE<gt>

Original version by Stefan Dragnev, E<lt>dragnev@molbio.mgh.harvard.eduE<gt>
with contributions from Norbert Kremer, E<lt>kremer@molbio.mgh.harvard.edu<gt>
and Danny Park, E<lt>park@molbio.mgh.harvard.edu.<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2004 by Sean P. Quinlan & Stefan Dragnev

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.3 or,
at your option, any later version of Perl 5 you may have available.


=cut
