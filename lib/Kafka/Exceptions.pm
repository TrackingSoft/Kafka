package Kafka::Exceptions;

=head1 NAME

Kafka::Exceptions - Perl Kafka API exception definitions.

=head1 VERSION

This documentation refers to C<Kafka::Exceptions> version 0.8002 .

=cut

#-- Pragmas --------------------------------------------------------------------

use 5.010;
use strict;
use warnings;

# ENVIRONMENT ------------------------------------------------------------------

our $VERSION = '0.8002';

use Exporter qw(
    import
);
our @EXPORT = qw(
    throw_args
);

#-- load the modules -----------------------------------------------------------

use Exception::Class (
    'Kafka::Exception' => {
        fields  => [ 'code', 'message' ],
    },
    'Kafka::Exception::Connection' => {
        isa     => 'Kafka::Exception',
    },
    'Kafka::Exception::Consumer' => {
        isa     => 'Kafka::Exception',
    },
    'Kafka::Exception::Int64' => {
        isa     => 'Kafka::Exception',
    },
    'Kafka::Exception::IO' => {
        isa     => 'Kafka::Exception',
    },
    'Kafka::Exception::Producer' => {
        isa     => 'Kafka::Exception',
    },
);

use Kafka qw(
    %ERROR
);

#-- declarations ---------------------------------------------------------------

=head1 SYNOPSIS

    use 5.010;
    use strict;
    use warnings;

    use Scalar::Util qw(
        blessed
    );
    use Try::Tiny;

    # A simple example of Kafka::Connection usage:
    use Kafka::Connection;

    # connect to local cluster with the defaults
    my $connection;
    try {
        $connection = Kafka::Connection->new( host => 'localhost' );
    } catch {
        if ( blessed( $_ ) && $_->isa( 'Kafka::Exception' ) ) {
            if ( $_->isa( 'Kafka::Exception::Connection' ) ) {
                # Specific treatment for 'Kafka::Connection' class error
            } elsif ( $_->isa( 'Kafka::Exception::IO' ) ) {
                # Specific treatment for 'Kafka::IO' class error
            }
            warn ref( $_ ), " error:\n", $_->message, "\n", $_->trace->as_string, "\n";
            exit;
        } else {
            die $_;
        }
    };

    # Closes the connection and cleans up
    undef $connection;

=head1 DESCRIPTION

The purpose of the C<Kafka::Exceptions> module is:

=over 3

=item *

Declare a Kafka API exceptions hierarchy.

=item *

Provide additional methods for working with exceptions.

=back

It is designed to make exception handling structured, simpler and better by encouraging use
of hierarchy of exceptions in application (vs single catch-all exception class).

The following additional attributes are available in C<Kafka::Exception> and its subclasses:

=over 3

=item C<code>

An error code that references error in C<%Kafka::ERROR> hash.

=item C<message>

An error message that contains information about the encountered failure.
This message may contain additional details which are not provided by C<%Kafka::ERROR> hash.

=back

Exception objects provide accessor methods for these attributes. Attributes are inherited by
subclasses.

Various Kafka API modules throw exceptions objects of a C<Kafka::Exception> subclass specific
to that module:

=over 3

=item C<Kafka::Exception::Connection>

See L<Kafka::Connection|Kafka::Connection> methods.

=item C<Kafka::Exception::Consumer>

See L<Kafka::Consumer|Kafka::Consumer> methods.

=item C<Kafka::Exception::IO>

See L<Kafka::IO|Kafka::IO> methods.

=item C<Kafka::Exception::Int64>

See L<Kafka::Int64|Kafka::Int64> methods.

=item C<Kafka::Exception::Producer>

See L<Kafka::Producer|Kafka::Producer> methods.

=back

Authors suggest using of L<Try::Tiny|Try::Tiny>'s C<try> and C<catch> to handle exceptions while
working with L<Kafka|Kafka> package.

You may also want to review documentation of L<Exception::Class|Exception::Class>,
which is the default base class for all exception objects created by this module.

=cut

#-- constructor ----------------------------------------------------------------

#-- public attributes ----------------------------------------------------------

=head2 FUNCTIONS

The following functions are exported by C<Kafka::Exceptions> module:

=cut

=head3 C<throw_args( $error_code, $description )>

Converts arguments into C<Kafka::Exception> constructor attributes L</code> and L</message>.

C<throw_args()> accepts the following arguments:

=over 3

=item C<$error_code>

The code of the last error.
The code must match the error codes defined in the module L<Kafka|Kafka>.

=item C<$description>

An additional error description that contains information about the encountered problem.

=back

=cut
sub throw_args {
    my( $error_code, $description ) = @_;

    return(
        code    => $error_code,
        message => $ERROR{ $error_code }.( $description ? ": $description" : q{} ),
    );
}

#-- private attributes ---------------------------------------------------------

#-- private methods ------------------------------------------------------------

#-- Closes and cleans up -------------------------------------------------------

1;

__END__

=head1 SEE ALSO

The basic operation of the Kafka package modules:

L<Kafka|Kafka> - constants and messages used by the Kafka package modules.

L<Kafka::Connection|Kafka::Connection> - interface to connect to a Kafka cluster.

L<Kafka::Producer|Kafka::Producer> - interface for producing client.

L<Kafka::Consumer|Kafka::Consumer> - interface for consuming client.

L<Kafka::Message|Kafka::Message> - interface to access Kafka message
properties.

L<Kafka::Int64|Kafka::Int64> - functions to work with 64 bit elements of the
protocol on 32 bit systems.

L<Kafka::Protocol|Kafka::Protocol> - functions to process messages in the
Apache Kafka's Protocol.

L<Kafka::IO|Kafka::IO> - low-level interface for communication with Kafka server.

L<Kafka::Exceptions|Kafka::Exceptions> - module designated to handle Kafka exceptions.

L<Kafka::Internals|Kafka::Internals> - internal constants and functions used
by several package modules.

A wealth of detail about the Apache Kafka and the Kafka Protocol:

Main page at L<http://kafka.apache.org/>

Kafka Protocol at L<https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol>

=head1 AUTHOR

Sergey Gladkov, E<lt>sgladkov@trackingsoft.comE<gt>

=head1 CONTRIBUTORS

Alexander Solovey

Jeremy Jordan

Sergiy Zuban

Vlad Marchenko

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012-2013 by TrackingSoft LLC.

This package is free software; you can redistribute it and/or modify it under
the same terms as Perl itself. See I<perlartistic> at
L<http://dev.perl.org/licenses/artistic.html>.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

=cut
