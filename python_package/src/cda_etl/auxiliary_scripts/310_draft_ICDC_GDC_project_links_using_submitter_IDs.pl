#!/usr/bin/env perl

use strict;

$| = 1;

# ARGUMENTS

my $icdc_file = shift;

my $gdc_file = shift;

my $out_file = shift;

die( "\n   Usage: $0 <ICDC file> <GDC file> <output file>\n\n" ) if ( not -e $icdc_file or not -e $gdc_file or $out_file eq '' );

# PARAMETERS

my $type_map = {
    
    'aliquot' => 'sample',
    'analyte' => 'sample',
    'case' => 'case',
    'diagnosis' => 'diagnosis',
    'portion' => 'sample',
    'sample' => 'sample',
    'slide' => 'sample'
};

# EXECUTION

open IN, "<$icdc_file" or die("Can't open $icdc_file for reading.\n");

my $header = <IN>;

my $icdc_data = {};

my $icdc_study_name_to_id = {};

my $icdc_study_to_program = {};

my $icdc_program_to_study = {};

my $icdc_program_acronym_to_name = {};

while ( chomp( my $line = <IN> ) ) {
    
    # program.program_name	program.program_acronym	study.clinical_study_designation	study.clinical_study_id	study.accession_id	study.clinical_study_name	entity_id	entity_type

    my ( $program_name, $program_acronym, $study_id, $study_clinical_study_id, $study_accession_id, $study_name, $entity_id, $entity_type ) = split( /\t/, $line );

    if ( exists( $type_map->{$entity_type} ) ) {
        
        my $target_type = $type_map->{$entity_type};

        $icdc_data->{$target_type}->{$entity_id}->{$study_name} = 1;

        $icdc_study_name_to_id->{$study_name} = $study_id;

        $icdc_study_to_program->{$study_name} = $program_acronym;

        $icdc_program_acronym_to_name->{$program_acronym} = $program_name;

        $icdc_program_to_study->{$program_acronym}->{$study_name} = 1;
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

my $icdc_program_total_match_count = {};

my $icdc_study_total_match_count = {};

my $icdc_to_gdc_count = {};

foreach my $target_type ( keys %$icdc_data ) {
    
    foreach my $entity_id ( keys %{$icdc_data->{$target_type}} ) {
        
        foreach my $study_name ( keys %{$icdc_data->{$target_type}->{$entity_id}} ) {
            
            if ( exists( $gdc_data->{$target_type} ) ) {
                
                if ( exists( $gdc_data->{$target_type}->{$entity_id} ) ) {
                    
                    foreach my $project_id ( keys %{$gdc_data->{$target_type}->{$entity_id}} ) {
                        
                        if ( exists( $icdc_to_gdc_count->{$study_name}->{$project_id} ) ) {
                            
                            $icdc_study_total_match_count->{$study_name} += 1;

                            $icdc_to_gdc_count->{$study_name}->{$project_id} += 1;

                        } else {
                            
                            $icdc_study_total_match_count->{$study_name} = 1;

                            $icdc_to_gdc_count->{$study_name}->{$project_id} = 1;
                        }

                        if ( exists( $icdc_program_total_match_count->{$icdc_study_to_program->{$study_name}} ) ) {
                            
                            $icdc_program_total_match_count->{$icdc_study_to_program->{$study_name}} += 1;

                        } else {
                            
                            $icdc_program_total_match_count->{$icdc_study_to_program->{$study_name}} = 1;
                        }
                    }
                }
            }
        }
    }
}

open OUT, ">$out_file" or die("Can't open $out_file for writing.\n");

print OUT join( "\t", 'match_count', 'ICDC_program_acronym', 'ICDC_program_name', 'ICDC_study_clinical_study_designation', 'ICDC_study_name', 'GDC_program_name', 'GDC_project_id' ) . "\n";

foreach my $program_acronym ( sort { $icdc_program_total_match_count->{$b} <=> $icdc_program_total_match_count->{$a} } keys %$icdc_program_total_match_count ) {
    
    my $program_name = $icdc_program_acronym_to_name->{$program_acronym};

    foreach my $study_name ( sort { $icdc_study_total_match_count->{$b} <=> $icdc_study_total_match_count->{$a} } keys %{$icdc_program_to_study->{$program_acronym}} ) {
        
        my $study_id = $icdc_study_name_to_id->{$study_name};

        foreach my $gdc_project_id ( sort { $a cmp $b } keys %{$icdc_to_gdc_count->{$study_name}} ) {
            
            my $match_count = $icdc_to_gdc_count->{$study_name}->{$gdc_project_id};

            my $gdc_program_name = $gdc_project_to_program->{$gdc_project_id};

            print OUT join( "\t", $match_count, $program_acronym, $program_name, $study_id, $study_name, $gdc_program_name, $gdc_project_id ) . "\n";
        }
    }
}

close OUT;


