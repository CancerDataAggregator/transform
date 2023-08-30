#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $cds_file = shift;

my $pdc_file = shift;

my $out_file = shift;

die( "\n   Usage: $0 <CDS file> <PDC file> <output file>\n\n" ) if ( not -e $cds_file or not -e $pdc_file or $out_file eq '' );

# PARAMETERS

my $type_map = {
    
    'aliquot' => 'sample',
    'analyte' => 'sample',
    'case' => 'case',
    'diagnosis' => 'diagnosis',
    'portion' => 'sample',
    'sample' => 'sample',
    'slide' => 'sample',

    'participant' => 'case'
};

# EXECUTION

open IN, "<$cds_file" or die("Can't open $cds_file for reading.\n");

my $header = <IN>;

my $cds_data = {};

my $cds_study_name_to_id = {};

my $cds_study_to_program = {};

my $cds_program_to_study = {};

while ( chomp( my $line = <IN> ) ) {
    
    # entity_type	entity_id	program_acronym	study_name	study_id

    my ( $entity_type, $entity_id, $program_acronym, $study_name, $study_id ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $cds_data->{$target_type}->{$entity_id}->{$study_name} = 1;

        $cds_study_name_to_id->{$study_name} = $study_id;

        $cds_study_to_program->{$study_name} = $program_acronym;

        $cds_program_to_study->{$program_acronym}->{$study_name} = 1;
    }
}

close IN;

open IN, "<$pdc_file" or die("Can't open $pdc_file for reading.\n");

$header = <IN>;

my $pdc_data = {};

my $pdc_study_to_project_and_program = {};

while ( chomp( my $line = <IN> ) ) {
    
    # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

    my ( $program_name, $project_submitter_id, $study_submitter_id, $pdc_study_id, $entity_submitter_id, $entity_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $pdc_data->{$target_type}->{$entity_submitter_id}->{$pdc_study_id} = 1;

        $pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}->{$project_submitter_id}->{$program_name} = 1;
    }
}

close IN;

my $cds_program_total_match_count = {};

my $cds_study_total_match_count = {};

my $cds_to_pdc_count = {};

foreach my $target_type ( keys %$cds_data ) {
    
    foreach my $entity_id ( keys %{$cds_data->{$target_type}} ) {
        
        foreach my $study_name ( keys %{$cds_data->{$target_type}->{$entity_id}} ) {
            
            if ( exists( $pdc_data->{$target_type} ) ) {
                
                if ( exists( $pdc_data->{$target_type}->{$entity_id} ) ) {
                    
                    foreach my $pdc_study_id ( keys %{$pdc_data->{$target_type}->{$entity_id}} ) {
                        
                        if ( exists( $cds_to_pdc_count->{$study_name}->{$pdc_study_id} ) ) {
                            
                            $cds_study_total_match_count->{$study_name} += 1;

                            $cds_to_pdc_count->{$study_name}->{$pdc_study_id} += 1;

                        } else {
                            
                            $cds_study_total_match_count->{$study_name} = 1;

                            $cds_to_pdc_count->{$study_name}->{$pdc_study_id} = 1;
                        }

                        if ( exists( $cds_program_total_match_count->{$cds_study_to_program->{$study_name}} ) ) {
                            
                            $cds_program_total_match_count->{$cds_study_to_program->{$study_name}} += 1;

                        } else {
                            
                            $cds_program_total_match_count->{$cds_study_to_program->{$study_name}} = 1;
                        }
                    }
                }
            }
        }
    }
}

open OUT, ">$out_file" or die("Can't open $out_file for writing.\n");

print OUT join( "\t", 'match_count', 'CDS_program_acronym', 'CDS_study_name', 'CDS_study_id', 'PDC_program_name', 'PDC_project_submitter_id', 'PDC_study_submitter_id', 'PDC_pdc_study_id' ) . "\n";

foreach my $program_acronym ( sort { $cds_program_total_match_count->{$b} <=> $cds_program_total_match_count->{$a} } keys %$cds_program_total_match_count ) {
    
    foreach my $study_name ( sort { $cds_study_total_match_count->{$b} <=> $cds_study_total_match_count->{$a} } keys %{$cds_program_to_study->{$program_acronym}} ) {
        
        my $study_id = $cds_study_name_to_id->{$study_name};

        foreach my $pdc_study_id ( sort { $a cmp $b } keys %{$cds_to_pdc_count->{$study_name}} ) {
            
            my $match_count = $cds_to_pdc_count->{$study_name}->{$pdc_study_id};

            foreach my $study_submitter_id ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}} ) {
                
                foreach my $project_id ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}} ) {
                    
                    foreach my $program_name ( sort { $a cmp $b } keys %{$pdc_study_to_project_and_program->{$pdc_study_id}->{$study_submitter_id}->{$project_id}} ) {
                        
                        print OUT join( "\t", $match_count, $program_acronym, $study_name, $study_id, $program_name, $project_id, $study_submitter_id, $pdc_study_id ) . "\n";
                    }
                }
            }
        }
    }
}

close OUT;


