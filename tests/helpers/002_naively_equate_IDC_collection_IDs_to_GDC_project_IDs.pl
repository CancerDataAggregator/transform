#!/usr/bin/perl

use strict;

$| = 1;

# PARAMETERS

my $idcFile = shift;

my $gdcFile = shift;

my $outFile = shift;

# EXECUTION

my $idToProject = {};

my $gdcProjectInProgram = {};

open IN, "<$gdcFile" or die("Can't open $gdcFile for reading.\n");

my $header = <IN>;

while ( chomp( my $line = <IN> ) ) {
    
    # entity_type	entity_submitter_id	GDC_project_id	GDC_program_name

    my ( $type, $id, $project, $program ) = split(/\t/, $line);

    $idToProject->{$type}->{$id}->{$project} = 1;

    $gdcProjectInProgram->{$project} = $program;
}

close IN;

open IN, "<$idcFile" or die("Can't open $idcFile for reading.\n");

$header = <IN>;

my $gdcProjectToIdcCollection = {};

while ( chomp( my $line = <IN> ) ) {
    
    # collection_id	submitter_case_id	idc_case_id	entity_type

    my ( $collection, $id, $case_id, $type ) = split(/\t/, $line);

    if ( exists( $idToProject->{$type}->{$id} ) ) {
        
        my $count = 0;

        foreach my $gdcProject ( keys %{$idToProject->{$type}->{$id}} ) {
            
            if ( not exists( $gdcProjectToIdcCollection->{$gdcProject}->{$collection} ) ) {
                
                $gdcProjectToIdcCollection->{$gdcProject}->{$collection} = 1;

                $count++;

            } else {
                
                $gdcProjectToIdcCollection->{$gdcProject}->{$collection} += 1;

                $count++;
            }

            if ( $count > 1 ) {
                
                print STDERR "WHAT?!\n";
            }
        }
    }
}

close IN;

open OUT, ">$outFile" or die("Can't open $outFile for writing.\n");

print OUT "match_count\tGDC_program_name\tGDC_project_id\tIDC_collection_id\n";

foreach my $gdcProject ( sort { $a cmp $b } keys %$gdcProjectToIdcCollection ) {
    
    foreach my $idcCollection ( sort { $a cmp $b } keys %{$gdcProjectToIdcCollection->{$gdcProject}} ) {
        
        print OUT "$gdcProjectToIdcCollection->{$gdcProject}->{$idcCollection}\t$gdcProjectInProgram->{$gdcProject}\t$gdcProject\t$idcCollection\n";
    }
}

close OUT;


