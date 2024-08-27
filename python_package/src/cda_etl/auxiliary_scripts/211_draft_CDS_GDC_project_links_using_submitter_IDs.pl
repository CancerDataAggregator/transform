#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $cds_file = shift;

my $gdc_file = shift;

my $out_file = shift;

die( "\n   Usage: $0 <CDS entity list> <GDC entity list> <output file>\n\n" ) if ( not -e $cds_file or not -e $gdc_file or $out_file eq '' );

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

my $cds_study_name_to_phs_accession = {};

my $cds_study_name_to_uuid = {};

my $cds_study_to_program = {};

my $cds_program_to_study = {};

while ( chomp( my $line = <IN> ) ) {
    
    # program.uuid	program.program_acronym	program.program_name	study.uuid	study.phs_accession	study.study_acronym	study.study_name	entity_submitter_id	entity_id	entity_type

    my ( $program_id, $program_acronym, $program_name, $study_id, $study_phs_accession, $study_acronym, $study_name, $entity_submitter_id, $entity_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $cds_data->{$target_type}->{$entity_submitter_id}->{$study_name} = 1;

        $cds_study_name_to_phs_accession->{$study_name} = $study_phs_accession;

        $cds_study_name_to_uuid->{$study_name} = $study_id;

        $cds_study_to_program->{$study_name} = $program_acronym;

        $cds_program_to_study->{$program_acronym}->{$study_name} = 1;
    }
}

close IN;

open IN, "<$gdc_file" or die("Can't open $gdc_file for reading.\n");

$header = <IN>;

my $gdc_data = {};

my $gdc_project_to_program = {};

while ( chomp( my $line = <IN> ) ) {
    
    # program.program_id	program.name	project.project_id	project.name	entity_submitter_id	entity_id	entity_type

    my ( $program_id, $program_name, $project_id, $project_name, $entity_submitter_id, $entity_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $gdc_data->{$target_type}->{$entity_submitter_id}->{$project_id} = 1;

        $gdc_project_to_program->{$project_id} = $program_name;
    }
}

close IN;

my $cds_program_total_match_count = {};

my $cds_study_total_match_count = {};

my $cds_to_gdc_count = {};

foreach my $target_type ( keys %$cds_data ) {
    
    foreach my $entity_id ( keys %{$cds_data->{$target_type}} ) {
        
        foreach my $study_name ( keys %{$cds_data->{$target_type}->{$entity_id}} ) {
            
            if ( exists( $gdc_data->{$target_type} ) ) {
                
                if ( exists( $gdc_data->{$target_type}->{$entity_id} ) ) {
                    
                    foreach my $project_id ( keys %{$gdc_data->{$target_type}->{$entity_id}} ) {
                        
                        if ( exists( $cds_to_gdc_count->{$study_name}->{$project_id} ) ) {
                            
                            $cds_study_total_match_count->{$study_name} += 1;

                            $cds_to_gdc_count->{$study_name}->{$project_id} += 1;

                        } else {
                            
                            $cds_study_total_match_count->{$study_name} = 1;

                            $cds_to_gdc_count->{$study_name}->{$project_id} = 1;
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

print OUT join( "\t", 'match_count', 'CDS_program_acronym', 'CDS_study_name', 'CDS_study_phs_accession', 'CDS_study_uuid', 'GDC_program_name', 'GDC_project_id' ) . "\n";

foreach my $program_acronym ( sort { $cds_program_total_match_count->{$b} <=> $cds_program_total_match_count->{$a} } keys %$cds_program_total_match_count ) {
    
    foreach my $study_name ( sort { $cds_study_total_match_count->{$b} <=> $cds_study_total_match_count->{$a} } keys %{$cds_program_to_study->{$program_acronym}} ) {
        
        foreach my $project_id ( sort { $a cmp $b } keys %{$cds_to_gdc_count->{$study_name}} ) {
            
            my $match_count = $cds_to_gdc_count->{$study_name}->{$project_id};

            my $program_name = $gdc_project_to_program->{$project_id};

            my $study_phs_accession = $cds_study_name_to_phs_accession->{$study_name};

            my $study_uuid = $cds_study_name_to_uuid->{$study_name};

            print OUT join( "\t", $match_count, $program_acronym, $study_name, $study_phs_accession, $study_uuid, $program_name, $project_id ) . "\n";
        }
    }
}

close OUT;


