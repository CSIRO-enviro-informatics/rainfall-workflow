#!/bin/bash 
# This shell script copies access data from nci and crops to ACCESS-R domain
# Note that NCO does both operations at once.
# while cdo needs to copy first using scp then crops and finally needs to be removed using rm.
# module load nco
#issueTime="00"
listDir=$(ssh ds4347@r-dm.nci.org.au ls /g/data/lb4/ops_aps2/access-g/0001/)
#listDir=(20160316 20160317 20160318 20160319 20160320)
#listDir=(20181115 20181116)
issueTimes='00
12'
for i in ${listDir[@]}
do 
	# echo $i
	yy="${i:0:4}"
    #echo $yy
	for ist in $issueTimes
		do
		outputDir="/OSM/CBR/LW_HYDROFCT/template/Data/Rainfall_forecasts/ACCESS_APS2/ACCESS_G_$ist""z/$yy"
		#echo $outputDir
		if [ ! -d $outputDir ]; then
			echo "The directory '$outputDir' does not exist, so creating it ..."   
			mkdir $outputDir
		fi	
		# only for cdo: outFileWthPath="$outputDir/ACCESS_G_whole_accum_prcp_fc_$i""$issueTime.nc"
		croppedFileWthPath="$outputDir/ACCESS_G_accum_prcp_fc_$i""$ist.nc"	
		#echo $outFileWthPath
		#echo $croppedFileWthPath
		if [ ! -f $croppedFileWthPath ]; then					
			nciSourceFileName="/g/data/lb4/ops_aps2/access-g/0001/$i/$ist""00/fc/sfc/accum_prcp.nc"
			#nciSourceFileName1="ds4347@r-dm.nci.org.au:"$nciSourceFileName
            #nciSourceFileName1=$(ssh ds4347@r-dm.nci.org.au ls $nciSourceFileName)
			echo "The file '$croppedFileWthPath' in not found, so copied from nci: $nciSourceFileName ..."        		
			#scp ds4347@r-dm.nci.org.au:$nciSourceFileName  $outFileWthPath		
			#echo "Croping '$outFileWthPath' to: $croppedFileWthPath ..."
			#cdo sellonlatbox,110,158,-45,-9 $outFileWthPath $croppedFileWthPath 
			#ncks -d lon,110.0,158.0 -d lat,-45.0,-9.0 $outFileWthPath $croppedFileWthPath	
			ssh_host="ds4347@r-dm.nci.org.au"			
			if ssh $ssh_host test -e $nciSourceFileName; then	
			    ncks -d lon,110.0,158.0 -d lat,-45.0,-9.0 ds4347@r-dm.nci.org.au:$nciSourceFileName -l $outputDir $croppedFileWthPath
				#echo "ACCESS file in nci '$nciSourceFileName' is found ..." 
			else
				echo "ACCESS file in nci '$nciSourceFileName' cannot be found ..."   
			fi
			#echo "Deleting '$outFileWthPath ..."
			#rm $outFileWthPath			
		fi
	done	
done
