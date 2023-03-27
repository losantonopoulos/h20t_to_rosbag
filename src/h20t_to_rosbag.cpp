#include <ros/ros.h>
#include "ros/init.h"

// System libraries
#include <stdbool.h>
#include <signal.h>
#include <stdio.h>
#include <cstdlib>
#include <iostream>
#include <chrono>
#include <thread>
#include <cstdint>
#include <fstream>
#include <cstring>
#include <mutex>
#include <ctime>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <math.h>
#include <limits>
#include <sstream>

// In order to produce bags
#include <rosbag/bag.h>

// image_transport includes
#include <image_transport/image_transport.h>

// OpenCV includes
#include <opencv2/core.hpp>
#include <opencv2/highgui.hpp>
#include <opencv2/imgproc.hpp>
#include <opencv2/videoio.hpp>  
#include <opencv2/core/mat.hpp>
#include <opencv2/opencv.hpp>
#include <cv_bridge/cv_bridge.h>

// tf2 includes
#include <tf2/LinearMath/Quaternion.h>

// Messages

// std_msgs
#include <std_msgs/Int32.h>
#include <std_msgs/String.h>
#include <std_msgs/UInt8.h>
#include <std_msgs/Float32.h>

// sensor_msgs
#include <sensor_msgs/image_encodings.h>
#include <sensor_msgs/Image.h>
#include <sensor_msgs/Imu.h>
#include <sensor_msgs/NavSatFix.h>

// geometry_msgs
#include <geometry_msgs/Quaternion.h>

// sense_msgs
#include <sense_msgs/UASComposite.h>
#include <sense_msgs/PositionWGS84Stamped.h>
#include <sense_msgs/RPYStamped.h>
#include <sense_msgs/RPY.h>

using namespace std;
using namespace cv;

double degToRad(double degrees)
{
    return (degrees * (double)M_PI) / 180.0;
}

double radToDeg(double radians)
{
    return (radians * 180.0) / (double)M_PI;
}

enum StreamType{

    INVALID = 0,
    WIDE,
    THERMAL,
    ZOOM
};

struct MetadataBasic{

    StreamType type;
    ros::Time current_timestamp, next_timestamp;
    double latitude, longitude, relative_altitude, absolute_altitude;
    double drone_speed_x, drone_speed_y, drone_speed_z;
    double drone_roll, drone_pitch, drone_yaw;
    sense_msgs::RPY gimbal_rpy[1];

    tf2::Quaternion uas_quaternion;
    tf2::Quaternion gimbal_quaternion[1];

    void toNavSatFixMsg(sensor_msgs::NavSatFix &gnss_msg, std_msgs::Header gnss_header){

        gnss_msg.header = gnss_header;

        gnss_msg.status.status  = 0;
        gnss_msg.status.service = 1;

        gnss_msg.latitude   = latitude;
        gnss_msg.longitude  = longitude;
        gnss_msg.altitude   = absolute_altitude;

        gnss_msg.position_covariance_type = sensor_msgs::NavSatFix::COVARIANCE_TYPE_UNKNOWN;
    }

    void toUASCompositeMsg(sense_msgs::UASComposite &uas_composite_msg, std_msgs::Header msg_header){

        // Header
        uas_composite_msg.header = msg_header;
        
        // Location related
        uas_composite_msg.position_wgs84.latitude          = latitude;
        uas_composite_msg.position_wgs84.longitude         = longitude;
        uas_composite_msg.position_wgs84.absolute_altitude = absolute_altitude;
        uas_composite_msg.position_wgs84.relative_altitude = relative_altitude;

        // Drone velocity related
        uas_composite_msg.velocity_linear.x = drone_speed_x; 
        uas_composite_msg.velocity_linear.y = drone_speed_y;
        uas_composite_msg.velocity_linear.z = drone_speed_z;

        // Drone orientation related
        uas_composite_msg.rotation_rpy.roll    = drone_roll;
        uas_composite_msg.rotation_rpy.pitch   = drone_pitch;
        uas_composite_msg.rotation_rpy.yaw     = drone_yaw;
        uas_composite_msg.rotation_quaternion.x   = uas_quaternion.getX();
        uas_composite_msg.rotation_quaternion.y   = uas_quaternion.getY();
        uas_composite_msg.rotation_quaternion.z   = uas_quaternion.getZ();
        uas_composite_msg.rotation_quaternion.w   = uas_quaternion.getW();

        // Gimbal orientation related
        uas_composite_msg.gimbal_rotation_rpy[0]   = gimbal_rpy[0];
        uas_composite_msg.gimbal_rotation_quaternion[0].x   = gimbal_quaternion[0].getX();
        uas_composite_msg.gimbal_rotation_quaternion[0].y   = gimbal_quaternion[0].getY();
        uas_composite_msg.gimbal_rotation_quaternion[0].z   = gimbal_quaternion[0].getZ();
        uas_composite_msg.gimbal_rotation_quaternion[0].w   = gimbal_quaternion[0].getW();
    }
};

struct MetadataInfo{
    
    bool available = false;
    std::string filename;
    ifstream *file;
};

struct Sequence {

    std::string filename;
    
    StreamType type;

    cv::VideoCapture capture;
  
    int total_frames = 0;

    int n_metadata_frames = 0;

    int curent_frame = 0;

    double fps = 0.0;

    bool completed = false;

    MetadataInfo metadata;
};

bool compareMetadataStamp(const MetadataBasic &metadata_1, const MetadataBasic &metadata_2)
{
    if(metadata_1.current_timestamp < metadata_2.current_timestamp ){
        return true;
    }else{
        return false;
    }
}

void releaseStreams(vector<Sequence> &seqs){

    // Release streams and files
    for (auto seq = seqs.begin(); seq != seqs.end(); ++seq){

        if(seq->capture.isOpened()) seq->capture.release();
            
        if(seq->metadata.file->is_open()) seq->metadata.file->close();
    }

    // Clean vector
    seqs.clear();

    return;
}

int parseSrt(ifstream *fs, StreamType stream_type, MetadataBasic *metadata){
    
    // Temp variable which will contain a single line form the .SRT file
    std::string line;

    // Parsing temp
    char arg_1[10], arg_2[10], arg_3[10], arg_4[10], arg_5[10], arg_6[10], arg_7[10], arg_8[10];
    int argint_1, argint_2, argint_3, argint_4, argint_5, argint_6, argint_7, argint_8;  
    double argdouble_1, argdouble_2, argdouble_3, argdouble_4, argdouble_5, argdouble_6, argdouble_7, argdouble_8;   
    size_t found;

    // How many variables were parsed
    int n_parsed = 0;

    // Current frame number (will be used in order to check for frame missmatch)
    int frame_num = 0;

    // Flags
    bool frame_mismatch = false;

    // Parsed metadata variables
    ros::Time cur_time, next_time;
    double lat, lon, rel_alt, abs_alt;
    double dr_speed_x, dr_speed_y, dr_speed_z;
    double dr_roll, dr_pitch, dr_yaw;
    double gb_roll, gb_pitch, gb_yaw;

    int frame_count = 0;

    // Scan the whole document
    while (!fs->eof()) {

        std::getline(*fs, line);

        found = line.find("-->");

        if (found != string::npos){ 
            
            // 00:00:00,033 --> 00:00:00,066
            n_parsed = sscanf(line.c_str(),"%d:%d:%d,%d --> %d:%d:%d,%d", &argint_1, &argint_2, &argint_3, &argint_4, &argint_5, &argint_6, &argint_7, &argint_8);
            //ROS_INFO("%d:%d:%d,%d --> %d:%d:%d,%d", argint_1, argint_2, argint_3, argint_4, argint_5, argint_6, argint_7, argint_8);
            
            if(n_parsed == 8){

                double secs_now  = (argint_4/1000.0) + argint_3 + (argint_2*60.0) + (argint_1*3600.0);
                double secs_next = (argint_8/1000.0) + argint_7 + (argint_6*60.0) + (argint_5*3600.0);

                ros::Duration time_offset(0.001);

                cur_time  = ros::Time(secs_now);  
                next_time = ros::Time(secs_next);

                // <font size="28">FrameCnt: 6, DiffTime: 34ms
                std::getline(*fs, line);

                found = line.find("FrameCnt");

                if(found != string::npos){

                    n_parsed = sscanf(line.c_str(),"<font size=\"%*d\">FrameCnt: %d", &frame_num);
                    //ROS_INFO("Frame: %d", frame_num);
                    
                    if(n_parsed == 1){

                        // 2023-03-10 18:12:51.441
                        std::getline(*fs, line);

                        // [fnum: 1.0] [focal_len: 58.00] [dzoom: 1.00] 
                        std::getline(*fs, line);

                        // [latitude: 35.527787] [longitude: 24.073218] [rel_alt: 224.715 abs_alt: 388.580] 
                        std::getline(*fs, line);

                        found = line.find("latitude");

                        if(found != string::npos){

                            n_parsed = sscanf(line.c_str(),"[latitude: %lf] [longitude: %lf] [rel_alt: %lf abs_alt: %lf]", &argdouble_1, &argdouble_2, &argdouble_3, &argdouble_4);
                            //ROS_INFO("Latitude: %lf Longitude %lf Rel_alt %lf Abs_alt %lf", argdouble_1, argdouble_2, argdouble_3, argdouble_4);

                            if(n_parsed == 4){

                                lat = argdouble_1;
                                lon = argdouble_2;
                                rel_alt = argdouble_3;
                                abs_alt = argdouble_4;

                                // [drone_speedx: 0.0 drone_speedy: 0.0 drone_speedz: 0.0] 
                                std::getline(*fs, line);

                                found = line.find("drone_speedx");

                                if(found != string::npos){

                                    n_parsed = sscanf(line.c_str(),"[drone_speedx: %lf drone_speedy: %lf drone_speedz: %lf]", &argdouble_1, &argdouble_2, &argdouble_3);
                                    //ROS_INFO("Drone_speedx: %lf Drone_speedy %lf Drone_speedz %lf", argdouble_1, argdouble_2, argdouble_3);

                                    if(n_parsed == 3){

                                        dr_speed_x = argdouble_1;
                                        dr_speed_y = argdouble_2;
                                        dr_speed_z = argdouble_3;

                                        // [drone_yaw: -129.9 drone_pitch: -6.0 drone_roll: 3.4]  
                                        std::getline(*fs, line);

                                        found = line.find("drone_yaw");

                                        if(found != string::npos){

                                            n_parsed = sscanf(line.c_str(),"[drone_yaw: %lf drone_pitch: %lf drone_roll: %lf]", &argdouble_1, &argdouble_2, &argdouble_3);
                                            //ROS_INFO("Drone_yaw: %lf Drone_Pitch %lf Drone_Roll %lf", argdouble_1, argdouble_2, argdouble_3);

                                            if(n_parsed == 3){
                                                
                                                dr_yaw = argdouble_1;
                                                dr_pitch = argdouble_2;
                                                dr_roll = argdouble_3;                                        

                                                // [gb_yaw: -137.9 gb_pitch: -36.4 gb_roll: 0.0]
                                                std::getline(*fs, line);

                                                found = line.find("gb_yaw");

                                                if(found != string::npos){

                                                    n_parsed = sscanf(line.c_str(),"[gb_yaw: %lf gb_pitch: %lf gb_roll: %lf]", &argdouble_1, &argdouble_2, &argdouble_3);
                                                    //ROS_INFO("Gimbal_yaw: %lf Gimbal_Pitch %lf Gimbal_Roll %lf", argdouble_1, argdouble_2, argdouble_3);

                                                    if(n_parsed == 3){

                                                        gb_yaw = argdouble_1;
                                                        gb_pitch = argdouble_2;
                                                        gb_roll = argdouble_3;

                                                        //ROS_INFO("Frame count: %d", frame_count);

                                                        // WARNING: The values coming from the UAS are in NED.  

                                                        metadata[frame_count].current_timestamp = cur_time;
                                                        metadata[frame_count].next_timestamp    = next_time;

                                                        metadata[frame_count].type = stream_type;

                                                        metadata[frame_count].latitude  = lat;
                                                        metadata[frame_count].longitude = lon;
                                                        metadata[frame_count].relative_altitude = rel_alt;
                                                        metadata[frame_count].absolute_altitude = abs_alt;

                                                        metadata[frame_count].drone_speed_x = dr_speed_x;
                                                        metadata[frame_count].drone_speed_y = dr_speed_y;
                                                        metadata[frame_count].drone_speed_z = dr_speed_z;

                                                        metadata[frame_count].drone_roll  = dr_roll;
                                                        metadata[frame_count].drone_pitch = dr_pitch;
                                                        metadata[frame_count].drone_yaw   = dr_yaw;
                                                        metadata[frame_count].uas_quaternion.setRPY(degToRad(dr_roll), degToRad(dr_pitch), degToRad(dr_yaw));

                                                        metadata[frame_count].gimbal_rpy[0].roll  = gb_roll;
                                                        metadata[frame_count].gimbal_rpy[0].pitch = gb_pitch;
                                                        metadata[frame_count].gimbal_rpy[0].yaw   = gb_yaw;
                                                        metadata[frame_count].gimbal_quaternion[0].setRPY(degToRad(gb_roll), degToRad(gb_pitch), degToRad(gb_yaw));

                                                        frame_count++;

                                                        if(frame_num != frame_count) frame_mismatch = true;
                                                    }
                                                    
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if(frame_mismatch) {
        ROS_WARN("Some metadata could not be parsed...");
    }

    return frame_count;  
}

bool tryAddStream(Sequence &seq, std::string f_prefix, StreamType type, vector<Sequence> &seqs){

    std::stringstream ss_file, ss_metadata_file;

    bool dash_included = (f_prefix[f_prefix.size()-1] == '_');

    switch(type){

        case StreamType::WIDE : {

            ss_file << f_prefix << (dash_included ? "W.MP4" : "_W.MP4");
            ss_metadata_file << f_prefix << (dash_included ? "W.SRT" : "_W.SRT");

            break;
        }

        case StreamType::THERMAL : {

            ss_file << f_prefix << (dash_included ? "T.MP4" : "_T.MP4");
            ss_metadata_file << f_prefix << (dash_included ? "T.SRT" : "_T.SRT");

            break;
        }

        case StreamType::ZOOM : {

            ss_file << f_prefix << (dash_included ? "Z.MP4" : "_Z.MP4");
            ss_metadata_file << f_prefix << (dash_included ? "Z.SRT" : "_Z.SRT");

            break;
        } 

        default:{

            return false; 
        }
    }

    //ROS_INFO("File: %s\nMetadata file: %s", ss_file.str().c_str(), ss_metadata_file.str().c_str());
    if(seq.capture.open(ss_file.str())){

        seq.total_frames = seq.capture.get(CAP_PROP_FRAME_COUNT);
        seq.fps = seq.capture.get(CAP_PROP_FPS);
        seq.completed = false;

        //ROS_INFO("Total frames: %d, fps: %f", seq.total_frames, seq.fps);

        seq.metadata.file->open(ss_metadata_file.str(), ios::in);
  
        if(seq.metadata.file->is_open()) {

            seq.metadata.filename = ss_metadata_file.str();            
            seq.metadata.available = true;            
        }     

        seqs.push_back(seq); 

        return true;
    }

    return false;
}

int main(int argc, char* argv[]){

    // Initialize node

    ros::init(argc, argv, "h20t_to_rosbag"); // Initialize the node
	ros::NodeHandle nh("~");

    // Get parameters

    std::string files_prefix;

    bool use_mono = false;

    // Get use mono (if true, all the streams will be saved as grayscale)
	if(!nh.getParam("use_mono", use_mono)) use_mono = false;

    ROS_INFO("Use mono: %s", (use_mono ? "true":"false"));

	// Read prefix
	
	if (!nh.getParam("prefix", files_prefix)){
		
		ROS_ERROR("You must supply the prefix for the required sequences: _prefix:='/aaa/bbb/DJI_1111'");
		ros::shutdown();

        return -1;
	}

	if(files_prefix.length() < 1){
		
		ROS_ERROR("You must supply a valid prefix for the required sequences: _prefix:='/aaa/bbb/DJI_1111'");
		ros::shutdown();

        return -1;
	}

    // Access video (.MP4) and metadata (.SRT) files

    vector<Sequence> sequences;
    sequences.reserve(4); // Reserve space for the streams

    bool use_wide = false;
    bool use_thermal = false;
    bool use_zoom  = false;

    // Try to add 'Wide' stream 
    
    Sequence seq_wide;
    ifstream metadata_file_wide;
    seq_wide.metadata.file = &metadata_file_wide;
    use_wide = tryAddStream(seq_wide, files_prefix, StreamType::WIDE, sequences);
    
    // Try to add 'Thermal' stream
    
    Sequence seq_thermal;
    ifstream metadata_file_thermal;
    seq_thermal.metadata.file = &metadata_file_thermal;
    use_thermal = tryAddStream(seq_thermal, files_prefix, StreamType::THERMAL, sequences);
    
    // Try to add 'Zoom' stream
    
    Sequence seq_zoom;
    ifstream metadata_file_zoom;
    seq_zoom.metadata.file = &metadata_file_zoom;
    use_zoom = tryAddStream(seq_zoom, files_prefix, StreamType::ZOOM, sequences);

    
    if(seq_wide.total_frames <= 0){

        ROS_WARN("Cannot access stream: Wide");
        use_wide = false;
    }else{

        ROS_INFO("Total frames detected in frame WIDE: %d @ %f fps", seq_wide.total_frames, seq_wide.fps);
    }

    if(seq_thermal.total_frames <= 0){

        ROS_WARN("Cannot access stream: Thermal");
        use_thermal = false;
    }else{

        ROS_INFO("Total frames detected in frame THERMAL: %d @ %f fps", seq_thermal.total_frames, seq_thermal.fps);
    }

    if(seq_zoom.total_frames <= 0){

        ROS_WARN("Cannot access stream: Zoom");
        use_zoom = false;
    }else{

        ROS_INFO("Total frames detected in frame ZOOM: %d @ %f fps", seq_zoom.total_frames, seq_zoom.fps);
    }
    
    // Allocate space for the metadata
    
    MetadataBasic wide_metadata[seq_wide.total_frames];
    MetadataBasic thermal_metadata[seq_thermal.total_frames];
    MetadataBasic zoom_metadata[seq_zoom.total_frames];

    // Get files information & Parse metadata
    
    int parsed_wide = 0; 
    int parsed_thermal = 0; 
    int parsed_zoom = 0;

    StreamType m300_metadata_source = StreamType::INVALID;

    if(use_wide) parsed_wide = parseSrt(seq_wide.metadata.file, StreamType::WIDE, wide_metadata);
    if(use_thermal) parsed_thermal = parseSrt(seq_thermal.metadata.file, StreamType::THERMAL, thermal_metadata);
    if(use_zoom) parsed_zoom = parseSrt(seq_zoom.metadata.file, StreamType::ZOOM, zoom_metadata);

    if(parsed_wide <= 0){

        ROS_WARN("Cannot parse metadata: Wide");

        // Create assumed timestamps
    }else{

        if(m300_metadata_source == StreamType::INVALID) m300_metadata_source = StreamType::WIDE;
    }

    if(parsed_thermal <= 0){

        ROS_WARN("Cannot parse metadata: Thermal");        

        // Create assumed timestamps
    }else{

        if(m300_metadata_source == StreamType::INVALID) m300_metadata_source = StreamType::THERMAL;
    }

    if(parsed_zoom <= 0){

        ROS_WARN("Cannot parse metadata:  Zoom");

        // Create assumed timestamps
    }else{

        if(m300_metadata_source == StreamType::INVALID) m300_metadata_source = StreamType::ZOOM;
    }

    int total_parsed = parsed_wide + parsed_thermal + parsed_zoom;

    vector<MetadataBasic> merged_metadata;
    merged_metadata.reserve(total_parsed);

    std::copy(wide_metadata, wide_metadata + parsed_wide, std::back_inserter(merged_metadata));
    std::copy(thermal_metadata, thermal_metadata + parsed_thermal, std::back_inserter(merged_metadata));
    std::copy(zoom_metadata, zoom_metadata + parsed_zoom, std::back_inserter(merged_metadata));

    std::sort(std::begin(merged_metadata), std::end(merged_metadata), &compareMetadataStamp);

    // Initialize bag

    rosbag::Bag bag;
    std::stringstream ss_out;

    if(files_prefix[files_prefix.size()-1] == '_'){
        ss_out << files_prefix.substr(0, files_prefix.size()-1) << ".bag";
    }else{
        ss_out << files_prefix << ".bag";
    }

    ROS_INFO("Initializing bag: %s", ss_out.str().c_str());    

    // Open bag file
    bag.open(ss_out.str(), rosbag::bagmode::Write);

    bool frames_available = false;

    Mat wide_frame, wide_frame_mono, thermal_frame, thermal_frame_mono, zoom_frame, zoom_frame_mono;

    cv_bridge::CvImage img_bridge;
    sensor_msgs::Image img_msg;

    //h20t_to_rosbag::M300Metadata m300_metadata_msg;

    sense_msgs::UASComposite uas_composite_msg;
    sense_msgs::RPY gimbal_rpy;

    gimbal_rpy.roll  = 0.0;
    gimbal_rpy.pitch = 0.0;
    gimbal_rpy.yaw   = 0.0;

    uas_composite_msg.gimbal_rotation_rpy.push_back(gimbal_rpy);

    geometry_msgs::Quaternion gimbal_quat;

    gimbal_quat.x = 0.0;
    gimbal_quat.y = 0.0;
    gimbal_quat.z = 0.0;
    gimbal_quat.w = 1.0;

    uas_composite_msg.gimbal_rotation_quaternion.push_back(gimbal_quat);
    
    sensor_msgs::NavSatFix gnss_msg;

    std_msgs::Header header_wide, header_thermal, header_zoom, header_uas, header_gnss; // empty header

    header_wide.seq     = 0;
    header_thermal.seq  = 0;
    header_zoom.seq     = 0;
    header_uas.seq      = 0;
    header_gnss.seq     = 0;

    header_wide.frame_id    = "h20t_wide";
    header_thermal.frame_id = "h20t_thermal";
    header_zoom.frame_id    = "h20t_zoom";
    header_uas.frame_id     = "m300_NED";
    header_gnss.frame_id    = "gnss";

    int counter = 0;

    double progress = 0.0;
    int int_progress = 0;

    bool should_exit = false;

    ros::Duration time_offset(0.001);

    // For every frame-step

    for (auto seq = merged_metadata.begin(); seq != merged_metadata.end(); ++seq){

        //ROS_INFO("Timestamp: %d.%d, type: %d, latitude: %lf", seq->current_timestamp.sec, seq->current_timestamp.nsec, seq->type, seq->latitude);

        switch(seq->type){

            case StreamType::WIDE : {

                try {

                    seq_wide.capture.read(wide_frame);
                    header_wide.stamp = seq->current_timestamp;

                    if(m300_metadata_source == StreamType::WIDE){

                        header_uas.stamp = seq->current_timestamp;
                        header_gnss.stamp = seq->current_timestamp;

                        seq->toUASCompositeMsg(uas_composite_msg, header_uas);
                                                
                        bag.write("/m300/uas_composite", header_uas.stamp + time_offset, uas_composite_msg);
                        header_uas.seq++;

                        seq->toNavSatFixMsg(gnss_msg, header_gnss);

                        bag.write("/m300/fix", header_gnss.stamp + time_offset, gnss_msg);
                        header_gnss.seq++;
                    }

                    if(!use_mono){

                        img_bridge = cv_bridge::CvImage(header_wide, sensor_msgs::image_encodings::BGR8, wide_frame);
                    }else{
                        
                        cv::cvtColor(wide_frame, wide_frame_mono, CV_BGR2GRAY);
                        img_bridge = cv_bridge::CvImage(header_wide, sensor_msgs::image_encodings::MONO8, wide_frame_mono);
                    }

                    img_bridge.toImageMsg(img_msg);                

                    bag.write("/h20t/wide/image_raw", header_wide.stamp + time_offset, img_msg);

                    header_wide.seq++;

                }catch (...) {

                    ROS_WARN("Invalid frame: WIDE");
                    continue;
                } 
           
                break;
            }

            case StreamType::THERMAL : {

                try {

                    seq_thermal.capture.read(thermal_frame);
                    header_thermal.stamp = seq->current_timestamp;

                    if(m300_metadata_source == StreamType::THERMAL){

                        header_uas.stamp = seq->current_timestamp;
                        header_gnss.stamp = seq->current_timestamp;

                        seq->toUASCompositeMsg(uas_composite_msg, header_uas);
                                                
                        bag.write("/m300/uas_composite", header_uas.stamp + time_offset, uas_composite_msg);
                        header_uas.seq++;

                        seq->toNavSatFixMsg(gnss_msg, header_gnss);

                        bag.write("/m300/fix", header_gnss.stamp + time_offset, gnss_msg);
                        header_gnss.seq++;
                    }

                    if(!use_mono){

                        img_bridge = cv_bridge::CvImage(header_thermal, sensor_msgs::image_encodings::BGR8, thermal_frame);
                    }else{
                        
                        cv::cvtColor(thermal_frame, thermal_frame_mono, CV_BGR2GRAY);
                        img_bridge = cv_bridge::CvImage(header_thermal, sensor_msgs::image_encodings::MONO8, thermal_frame_mono);
                    }

                    img_bridge.toImageMsg(img_msg);  

                    bag.write("/h20t/thermal/image_raw", header_thermal.stamp + time_offset, img_msg);

                    header_thermal.seq = header_thermal.seq + 1;

                }catch (...) {
                    
                    ROS_WARN("Invalid frame: THERMAL");
                    continue;
                } 

                break;
            }

            case StreamType::ZOOM : {

                try {

                    seq_zoom.capture.read(zoom_frame);
                    header_zoom.stamp = seq->current_timestamp;

                    if(m300_metadata_source == StreamType::ZOOM){

                        header_uas.stamp = seq->current_timestamp;
                        header_gnss.stamp = seq->current_timestamp;

                        seq->toUASCompositeMsg(uas_composite_msg, header_uas);
                                                
                        bag.write("/m300/uas_composite", header_uas.stamp + time_offset, uas_composite_msg);
                        header_uas.seq++;

                        seq->toNavSatFixMsg(gnss_msg, header_gnss);

                        bag.write("/m300/fix", header_gnss.stamp + time_offset, gnss_msg);
                        header_gnss.seq++;
                    }

                    if(!use_mono){

                        img_bridge = cv_bridge::CvImage(header_zoom, sensor_msgs::image_encodings::BGR8, zoom_frame);
                    }else{
                        
                        cv::cvtColor(zoom_frame, zoom_frame_mono, CV_BGR2GRAY);
                        img_bridge = cv_bridge::CvImage(header_zoom, sensor_msgs::image_encodings::MONO8, zoom_frame_mono);
                    }

                    img_bridge.toImageMsg(img_msg);  

                    bag.write("/h20t/zoom/image_raw", header_zoom.stamp + time_offset, img_msg);    

                    header_zoom.seq = header_zoom.seq + 1;

                }catch (...) {
                    
                    ROS_WARN("Invalid frame: ZOOM");
                    continue;
                }     

                break;
            }

            default : {
                
                ROS_ERROR("Parsing related error occured");
                break;
            }
        }

        counter++;

        progress = 100.0 * (((double)counter)/((double)total_parsed));
        int_progress = (int)std::ceil(progress);

        if(int_progress % 2 == 0) std::cout << "Progress: " << int_progress << "% \t\r" << std::flush;

        if(should_exit) break; 
    }

    bag.close();

    // Clean up
    releaseStreams(sequences);
    merged_metadata.clear();

    // Return OK - Done
    return 0;
}
